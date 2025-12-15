//! HLS implementation of the [`SourceStream`] trait.
//!
//! This module provides `HlsStream`, a `SourceStream` implementation for HLS streams.
//! It handles playlist parsing, segment downloading, and adaptive bitrate switching.
//!
//! The stream produces raw bytes from HLS media segments, including init segments
//! when available. The stream concatenates init segment data with media segment data
//! to ensure proper decoding.
//!
//! # Performance Considerations for Mobile Devices
//!
//! - Uses `Arc` for shared configuration to avoid deep cloning
//! - Minimizes string allocations in hot paths
//! - Uses bounded channels for backpressure control
//! - Implements proper cancellation with `CancellationToken`
//! - Avoids mutex in poll_next for better async performance

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{self, Poll};

use bytes::Bytes;
use futures_util::Stream;
use tokio::sync::mpsc::{self, error::SendError};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, trace};

use crate::abr::AbrConfig;
use crate::{
    AbrController, Aes128CbcMiddleware, HlsManager, MediaStream, ResourceDownloader,
    StreamMiddleware, apply_middlewares,
};
use stream_download::source::{DecodeError, SourceStream};
use stream_download::storage::ContentLength;

/// Parameters for creating an HLS stream.
#[derive(Debug, Clone)]
pub struct HlsStreamParams {
    /// The URL of the HLS master playlist.
    pub url: Arc<str>,
    /// Unified settings for HLS playback and downloader behavior.
    pub settings: Arc<crate::HlsSettings>,
}

impl HlsStreamParams {
    /// Create new HLS stream parameters.
    pub fn new(url: impl Into<Arc<str>>, settings: impl Into<Arc<crate::HlsSettings>>) -> Self {
        Self {
            url: url.into(),
            settings: settings.into(),
        }
    }
}

/// Error type for HLS stream creation and operations.
#[derive(Debug)]
pub enum HlsStreamError {
    /// IO error occurred.
    Io(Arc<io::Error>),
    /// HLS-specific error occurred.
    Hls(HlsErrorKind),
    /// Invalid parameters provided.
    InvalidParams(&'static str),
    /// Stream was cancelled.
    Cancelled,
}

/// Specific HLS error kinds to avoid string allocations.
#[derive(Debug)]
pub enum HlsErrorKind {
    /// Failed to load master playlist.
    MasterPlaylistLoad(crate::model::HlsError),
    /// No variants available in master playlist.
    NoVariants,
    /// Failed to initialize ABR controller.
    ControllerInit(crate::model::HlsError),
    /// Failed to send seek command.
    SeekFailed,
    /// Streaming loop not initialized.
    StreamingLoopNotInitialized,
    /// Backward seek not supported.
    BackwardSeekNotSupported,
}

impl Display for HlsStreamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HlsStreamError::Io(err) => write!(f, "IO error: {}", err),
            HlsStreamError::Hls(kind) => match kind {
                HlsErrorKind::MasterPlaylistLoad(e) => {
                    write!(f, "Failed to load master playlist: {}", e)
                }
                HlsErrorKind::NoVariants => write!(f, "No variants available in master playlist"),
                HlsErrorKind::ControllerInit(e) => {
                    write!(f, "Failed to initialize ABR controller: {}", e)
                }
                HlsErrorKind::SeekFailed => write!(f, "Failed to send seek command"),
                HlsErrorKind::StreamingLoopNotInitialized => {
                    write!(f, "Streaming loop not initialized")
                }
                HlsErrorKind::BackwardSeekNotSupported => {
                    write!(f, "Backward seek not supported")
                }
            },
            HlsStreamError::InvalidParams(msg) => write!(f, "Invalid parameters: {}", msg),
            HlsStreamError::Cancelled => write!(f, "Stream was cancelled"),
        }
    }
}

impl Error for HlsStreamError {}

impl DecodeError for HlsStreamError {
    async fn decode_error(self) -> String {
        self.to_string()
    }
}

impl From<io::Error> for HlsStreamError {
    fn from(err: io::Error) -> Self {
        HlsStreamError::Io(Arc::new(err))
    }
}

impl From<SendError<u64>> for HlsStreamError {
    fn from(_: SendError<u64>) -> Self {
        HlsStreamError::Hls(HlsErrorKind::SeekFailed)
    }
}

/// HLS implementation of the [`SourceStream`] trait.
///
/// This stream handles HLS playback including:
/// - Master playlist parsing
/// - Media playlist updates
/// - Segment downloading
/// - Adaptive bitrate switching
/// - Limited seeking support
///
/// The stream produces raw bytes from HLS media segments. Higher-level components
/// are responsible for handling init segments and variant switching metadata.
/// Events emitted by the HLS streaming pipeline (out-of-band metadata).
#[derive(Clone, Debug)]
pub enum StreamEvent {
    /// ABR: a new variant (rendition) has been selected. Emitted before the init segment.
    VariantChanged {
        variant_id: crate::model::VariantId,
        codec_info: Option<crate::model::CodecInfo>,
    },
    /// Beginning of an init segment in the main byte stream.
    /// byte_len may be None when exact length is unknown (e.g., due to DRM/middleware).
    InitStart {
        variant_id: crate::model::VariantId,
        codec_info: Option<crate::model::CodecInfo>,
        byte_len: Option<u64>,
    },
    /// End of the init segment in the main byte stream.
    InitEnd { variant_id: crate::model::VariantId },
    /// Optional: media segment boundaries (useful for metrics).
    SegmentStart {
        sequence: u64,
        variant_id: crate::model::VariantId,
        byte_len: Option<u64>,
        duration: std::time::Duration,
    },
    SegmentEnd {
        sequence: u64,
        variant_id: crate::model::VariantId,
    },
}

pub struct HlsStream {
    /// Channel receiver for stream data.
    data_receiver: mpsc::Receiver<Bytes>,
    /// Channel sender for seek commands to the streaming loop.
    seek_sender: mpsc::Sender<u64>,
    /// Cancellation token for graceful shutdown.
    cancel_token: CancellationToken,
    /// Task handle for background streaming.
    streaming_task: tokio::task::JoinHandle<()>,
    /// Whether the stream has finished (end of VOD stream reached).
    finished: bool,
    /// Event broadcaster for out-of-band stream events.
    event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
}

struct HlsStreamWorker {
    data_sender: mpsc::Sender<Bytes>,
    seek_receiver: mpsc::Receiver<u64>,
    cancel_token: CancellationToken,
    event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
    streaming_downloader: ResourceDownloader,
    controller: AbrController<HlsManager>,
    current_position: u64,
    bytes_to_skip: u64,
    retry_delay: std::time::Duration,
}

impl HlsStreamWorker {
    /// Build per-segment middlewares (no self borrow to avoid conflicts).
    #[inline]
    fn build_middlewares(
        drm_params_opt: Option<([u8; 16], [u8; 16])>,
    ) -> Vec<Arc<dyn StreamMiddleware>> {
        let mut v: Vec<Arc<dyn StreamMiddleware>> = Vec::new();

        #[cfg(feature = "aes-decrypt")]
        if let Some((key, iv)) = drm_params_opt {
            v.push(Arc::new(Aes128CbcMiddleware::new(key, iv)));
        }

        v
    }
    const INITIAL_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
    const MAX_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

    /// Emit events for variant changes and segment boundaries before streaming bytes.
    /// Keeps `run` method concise and standardizes event ordering.
    #[inline]
    fn emit_pre_segment_events(
        &self,
        last_variant_id: &mut Option<crate::model::VariantId>,
        desc: &crate::manager::SegmentDescriptor,
        seg_size: Option<u64>,
        init_len_opt: Option<u64>,
    ) {
        if *last_variant_id != Some(desc.variant_id) {
            let _ = self.event_sender.send(StreamEvent::VariantChanged {
                variant_id: desc.variant_id,
                codec_info: desc.codec_info.clone(),
            });
            *last_variant_id = Some(desc.variant_id);
        }
        if desc.is_init {
            let _ = self.event_sender.send(StreamEvent::InitStart {
                variant_id: desc.variant_id,
                codec_info: desc.codec_info.clone(),
                byte_len: init_len_opt,
            });
        } else {
            let _ = self.event_sender.send(StreamEvent::SegmentStart {
                sequence: desc.sequence,
                variant_id: desc.variant_id,
                byte_len: seg_size,
                duration: desc.duration,
            });
        }
    }

    /// Emit post-segment boundary events after streaming completes.
    #[inline]
    fn emit_post_segment_events(&self, desc: &crate::manager::SegmentDescriptor) {
        if desc.is_init {
            let _ = self.event_sender.send(StreamEvent::InitEnd {
                variant_id: desc.variant_id,
            });
        } else {
            let _ = self.event_sender.send(StreamEvent::SegmentEnd {
                sequence: desc.sequence,
                variant_id: desc.variant_id,
            });
        }
    }

    /// Apply a seek position updating internal byte counters and manager state.
    #[instrument(skip(self), fields(position))]
    async fn apply_seek_position(&mut self, position: u64) -> Result<(), HlsStreamError> {
        if position >= self.current_position {
            self.bytes_to_skip = position - self.current_position;
        } else {
            let (desc, intra) = self
                .controller
                .inner_stream_mut()
                .resolve_position(position)
                .await
                .map_err(|_| HlsStreamError::Hls(HlsErrorKind::SeekFailed))?;
            self.controller
                .inner_stream_mut()
                .seek_to_descriptor(&desc)
                .map_err(|_| HlsStreamError::Hls(HlsErrorKind::SeekFailed))?;
            self.bytes_to_skip = intra;
            self.current_position = position.saturating_sub(intra);
        }
        Ok(())
    }

    /// Compute segment size for skip math, probing when unknown.
    #[instrument(skip(self, desc))]
    async fn compute_segment_size(
        &mut self,
        desc: &crate::manager::SegmentDescriptor,
    ) -> Option<u64> {
        let seg_size_opt = if desc.is_init {
            self.controller.inner_stream().segment_size(0)
        } else {
            self.controller.inner_stream().segment_size(desc.sequence)
        };
        if let Some(sz) = seg_size_opt {
            Some(sz)
        } else {
            match self
                .controller
                .inner_stream_mut()
                .probe_and_record_segment_size(
                    if desc.is_init { 0 } else { desc.sequence },
                    &desc.uri,
                )
                .await
            {
                Ok(s) => s,
                Err(_) => None,
            }
        }
    }

    /// Resolve DRM (AES-128-CBC) params for a descriptor if applicable.
    #[instrument(skip(self, desc))]
    async fn resolve_drm_params_for_desc(
        &mut self,
        desc: &crate::manager::SegmentDescriptor,
    ) -> Option<([u8; 16], [u8; 16])> {
        self.controller
            .inner_stream_mut()
            .resolve_aes128_cbc_params(
                desc.key.as_ref(),
                if desc.is_init {
                    None
                } else {
                    Some(desc.sequence)
                },
            )
            .await
            .ok()
            .flatten()
    }

    /// Handle live refresh wait with cancellation and seek support.
    #[instrument(skip(self), fields(wait = ?wait))]
    async fn handle_needs_refresh(
        &mut self,
        wait: std::time::Duration,
    ) -> Result<(), HlsStreamError> {
        tokio::select! {
            biased;
            _ = self.cancel_token.cancelled() => {
                return Err(HlsStreamError::Cancelled);
            }
            seek = self.seek_receiver.recv() => {
                if let Some(position) = seek {
                    tracing::trace!("HLS streaming loop: received seek during wait");
                    self.apply_seek_position(position).await?;
                }
                // Continue regardless
            }
            _ = tokio::time::sleep(wait) => {
                tracing::trace!("HLS stream: live refresh wait completed");
            }
        }
        Ok(())
    }

    /// Sleep with backoff and cancellation; update retry delay.
    #[instrument(skip(self))]
    async fn backoff_sleep(&mut self) -> Result<(), HlsStreamError> {
        tokio::select! {
            biased;
            _ = self.cancel_token.cancelled() => {
                return Err(HlsStreamError::Cancelled);
            }
            _ = tokio::time::sleep(self.retry_delay) => {}
        }
        self.retry_delay = (self.retry_delay * 2).min(Self::MAX_RETRY_DELAY);
        Ok(())
    }

    /// Pump bytes from a segment stream to downstream with backpressure, cancellation and seek handling.
    #[instrument(skip(self, stream))]
    async fn pump_stream_chunks(
        &mut self,
        mut stream: crate::HlsByteStream,
    ) -> Result<(), HlsStreamError> {
        // Reserve on demand to avoid long-lived borrows during prefetch phases
        let mut first_permit: Option<tokio::sync::mpsc::Permit<Bytes>> = None;

        loop {
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    return Err(HlsStreamError::Cancelled);
                }
                // Concurrently react to a seek during streaming a segment
                seek = self.seek_receiver.recv() => {
                    if let Some(position) = seek {
                        tracing::trace!("HLS streaming loop: received seek during streaming");
                        self.apply_seek_position(position).await?;
                        // break to apply new position on next iteration
                        break;
                    } else {
                        tracing::trace!("HLS stream: seek channel closed");
                        break;
                    }
                }
                item = futures_util::StreamExt::next(&mut stream) => {
                    match item {
                        Some(Ok(chunk)) => {
                            // Send chunk
                            let permit = match first_permit.take() {
                                Some(p) => p,
                                None => match self.data_sender.reserve().await {
                                    Ok(p) => p,
                                    Err(_) => { tracing::trace!("HLS stream: receiver dropped, stopping"); break; }
                                },
                            };
                            let len = chunk.len() as u64;
                            permit.send(chunk);
                            self.current_position = self.current_position.saturating_add(len);
                        }
                        Some(Err(e)) => {
                            tracing::error!("Error reading segment chunk: {}, retrying in {:?}", e, self.retry_delay);
                            self.backoff_sleep().await?;
                            break;
                        }
                        None => {
                            // Segment finished
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a single segment descriptor: computes size, handles skip, DRM, events and streaming.
    #[instrument(skip(self, last_variant_id))]
    async fn process_descriptor(
        &mut self,
        desc: crate::manager::SegmentDescriptor,
        last_variant_id: &mut Option<crate::model::VariantId>,
    ) -> Result<(), HlsStreamError> {
        // Determine segment size if needed for skip math
        let seg_size = self.compute_segment_size(&desc).await;

        // Handle full-segment skip if we have enough bytes_to_skip and know size
        if let (Some(size), true) = (seg_size, self.bytes_to_skip > 0) {
            if self.bytes_to_skip >= size {
                self.bytes_to_skip -= size;
                self.current_position += size;
                return Ok(());
            }
        }

        // Resolve DRM (AES-128-CBC) parameters before emitting events or building middlewares
        let drm_params_opt = self.resolve_drm_params_for_desc(&desc).await;
        let drm_applied = drm_params_opt.is_some();

        // Emit events around init boundaries and variant changes
        let init_len_opt = if desc.is_init {
            if drm_applied { None } else { seg_size }
        } else {
            None
        };
        self.emit_pre_segment_events(last_variant_id, &desc, seg_size, init_len_opt);

        // Build a stream according to skip and init rules
        let stream_res = if self.bytes_to_skip > 0 && !desc.is_init {
            let start = self.bytes_to_skip;
            self.bytes_to_skip = 0;
            self.streaming_downloader
                .stream_segment_range(&desc.uri, start, None)
                .await
        } else {
            // For init segments, we always send the full segment even if skip is inside it
            self.streaming_downloader.stream_segment(&desc.uri).await
        };

        let stream = match stream_res {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    "Failed to open segment stream: {}, retrying in {:?}",
                    e,
                    self.retry_delay
                );
                self.backoff_sleep().await?;
                return Ok(());
            }
        };

        // Apply streaming middlewares (DRM, etc.) and pump data
        let middlewares = HlsStreamWorker::build_middlewares(drm_params_opt);
        let stream = apply_middlewares(stream, &middlewares);
        self.pump_stream_chunks(stream).await?;

        // Emit segment boundary events after finishing streaming
        self.emit_post_segment_events(&desc);

        Ok(())
    }

    async fn new(
        url: Arc<str>,
        settings: Arc<crate::HlsSettings>,
        data_sender: mpsc::Sender<Bytes>,
        seek_receiver: mpsc::Receiver<u64>,
        cancel_token: CancellationToken,
        event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
    ) -> Result<Self, HlsStreamError> {
        // Build dedicated downloaders from flattened settings
        let (request_timeout, max_retries, retry_base_delay, max_retry_delay) = (
            settings.request_timeout,
            settings.max_retries,
            settings.retry_base_delay,
            settings.max_retry_delay,
        );

        let streaming_downloader = ResourceDownloader::new(
            request_timeout,
            max_retries,
            retry_base_delay,
            max_retry_delay,
        );
        let manager_downloader = ResourceDownloader::new(
            request_timeout,
            max_retries,
            retry_base_delay,
            max_retry_delay,
        );

        let mut manager = HlsManager::new(url.clone(), settings.clone(), manager_downloader);

        // Initialize manager and ABR controller
        manager
            .load_master()
            .await
            .map_err(|e| HlsStreamError::Hls(HlsErrorKind::MasterPlaylistLoad(e)))?;

        let master = manager
            .master()
            .ok_or_else(|| HlsStreamError::Hls(HlsErrorKind::StreamingLoopNotInitialized))?;

        let initial_variant_index = {
            if master.variants.is_empty() {
                return Err(HlsStreamError::Hls(HlsErrorKind::NoVariants));
            }
            match settings.selection_manual_variant_id {
                None => manager.current_variant_index().unwrap_or(0),
                Some(index) => index.0,
            }
        };

        let init_bw = master.variants[initial_variant_index]
            .bandwidth
            .unwrap_or(0) as f64;

        let abr_cfg = AbrConfig {
            min_buffer_for_up_switch: settings.abr_min_buffer_for_up_switch,
            down_switch_buffer: settings.abr_down_switch_buffer,
            throughput_safety_factor: settings.abr_throughput_safety_factor,
            up_hysteresis_ratio: settings.abr_up_hysteresis_ratio,
            down_hysteresis_ratio: settings.abr_down_hysteresis_ratio,
            min_switch_interval: settings.abr_min_switch_interval,
        };
        let manual_variant_id = settings.selection_manual_variant_id;

        let mut controller = AbrController::new(
            manager,
            abr_cfg,
            manual_variant_id,
            initial_variant_index,
            init_bw,
        );

        controller
            .init()
            .await
            .map_err(|e| HlsStreamError::Hls(HlsErrorKind::ControllerInit(e)))?;

        Ok(Self {
            data_sender,
            seek_receiver,
            cancel_token,
            event_sender,
            streaming_downloader,
            controller,
            current_position: 0,
            bytes_to_skip: 0,
            retry_delay: Self::INITIAL_RETRY_DELAY,
        })
    }

    #[instrument(skip(self))]
    async fn run(mut self) -> Result<(), HlsStreamError> {
        let mut last_variant_id: Option<crate::model::VariantId> = None;
        loop {
            // Fetch next segment descriptor using non-blocking method
            let next_desc = tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    return Err(HlsStreamError::Cancelled);
                }
                seek = self.seek_receiver.recv() => {
                    if let Some(position) = seek {
                        tracing::trace!("HLS streaming loop: received seek to position {}", position);
                        self.apply_seek_position(position).await?;
                        // no pre-reserved permit; continue
                        continue;
                    } else {
                        tracing::trace!("HLS stream: seek channel closed");
                        break;
                    }
                }
                result = self.controller.inner_stream_mut().next_segment_descriptor_nonblocking() => result,
            };

            match next_desc {
                Ok(crate::manager::NextSegmentDescResult::Segment(desc)) => {
                    // Reset retry delay on success
                    self.retry_delay = Self::INITIAL_RETRY_DELAY;

                    // Delegate per-segment logic to a dedicated helper
                    self.process_descriptor(desc, &mut last_variant_id).await?;
                }
                Ok(crate::manager::NextSegmentDescResult::EndOfStream) => {
                    tracing::trace!("HLS stream: end of stream");
                    break;
                }
                Ok(crate::manager::NextSegmentDescResult::NeedsRefresh { wait }) => {
                    // Live stream needs to wait for new segments
                    // Use select! for proper cancellation during wait
                    // no pre-reserved permit to drop
                    self.handle_needs_refresh(wait).await?;
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to get next segment descriptor: {}, retrying in {:?}",
                        e,
                        self.retry_delay
                    );

                    self.backoff_sleep().await?;
                }
            }
        }

        Ok(())
    }
}

impl HlsStream {
    /// Create a new HLS stream.
    ///
    /// # Arguments
    /// * `url` - URL of the HLS master playlist
    /// * `config` - HLS configuration
    /// * `abr_config` - ABR configuration
    /// * `selection_mode` - Selection mode for variants
    pub async fn new(
        url: impl Into<Arc<str>>,
        settings: Arc<crate::HlsSettings>,
    ) -> Result<Self, HlsStreamError> {
        Self::new_with_config(url, settings).await
    }

    /// Create a new HLS stream with custom downloader configuration.
    ///
    /// # Arguments
    /// * `url` - URL of the HLS master playlist
    /// * `config` - HLS configuration
    /// * `abr_config` - ABR configuration
    /// * `selection_mode` - Selection mode for variants
    /// * `downloader_config` - Optional custom downloader configuration
    pub async fn new_with_config(
        url: impl Into<Arc<str>>,
        settings: Arc<crate::HlsSettings>,
    ) -> Result<Self, HlsStreamError> {
        let url = url.into();

        // Create channels for data and commands
        // Use bounded channel for data to control backpressure with configurable buffer size
        let buffer_size = settings.prefetch_buffer_size;
        let (data_sender, data_receiver) = mpsc::channel(buffer_size);
        let (seek_sender, seek_receiver) = mpsc::channel(1);
        let cancel_token = CancellationToken::new();

        // Create event broadcast channel (out-of-band metadata)
        let (event_sender, _event_receiver) = tokio::sync::broadcast::channel(64);

        // Start background streaming task
        let streaming_task = Self::start_streaming_task(
            url,
            settings,
            data_sender,
            seek_receiver,
            cancel_token.clone(),
            event_sender.clone(),
        )
        .await?;

        Ok(Self {
            data_receiver,
            seek_sender,
            cancel_token,
            streaming_task,
            finished: false,
            event_sender,
        })
    }

    /// Subscribe to out-of-band stream events (variant changes, init boundaries, etc).
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<StreamEvent> {
        self.event_sender.subscribe()
    }

    /// Start the background streaming task.
    async fn start_streaming_task(
        url: Arc<str>,
        settings: Arc<crate::HlsSettings>,
        data_sender: mpsc::Sender<Bytes>,
        seek_receiver: mpsc::Receiver<u64>,
        cancel_token: CancellationToken,
        event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
    ) -> Result<tokio::task::JoinHandle<()>, HlsStreamError> {
        let task = tokio::spawn(async move {
            tracing::trace!("HLS streaming task started");
            if let Err(e) = Self::streaming_loop(
                url,
                settings,
                data_sender,
                seek_receiver,
                cancel_token,
                event_sender.clone(),
            )
            .await
            {
                match e {
                    HlsStreamError::Cancelled => {
                        tracing::trace!("HLS streaming task cancelled")
                    }
                    _ => tracing::error!("HLS streaming loop error: {}", e),
                }
            }
            tracing::trace!("HLS streaming task finished");
        });

        Ok(task)
    }

    /// Main streaming loop that runs in a background task.
    ///
    /// This loop uses non-blocking segment fetching with proper cancellation
    /// support via `tokio::select!`. No `spawn_blocking` or hidden sleeps.
    async fn streaming_loop(
        url: Arc<str>,
        settings: Arc<crate::HlsSettings>,
        data_sender: mpsc::Sender<Bytes>,
        seek_receiver: mpsc::Receiver<u64>,
        cancel_token: CancellationToken,
        event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
    ) -> Result<(), HlsStreamError> {
        let worker = HlsStreamWorker::new(
            url,
            settings,
            data_sender,
            seek_receiver,
            cancel_token,
            event_sender,
        )
        .await?;
        worker.run().await
    }

    /// Seek to a specific position in the stream.
    ///
    /// Position is an absolute byte offset from the beginning of the concatenated stream.
    /// Supports forward and backward seeks. Backward seek will reset the internal controller
    /// state and replay from the start using byte-range where possible.
    async fn seek_internal(&self, position: u64) -> Result<(), HlsStreamError> {
        self.seek_sender.send(position).await?;
        Ok(())
    }
}

impl Drop for HlsStream {
    fn drop(&mut self) {
        // Cancel the streaming task
        self.cancel_token.cancel();

        // Abort the task if it's still running
        if !self.streaming_task.is_finished() {
            self.streaming_task.abort();
        }
    }
}

impl SourceStream for HlsStream {
    type Params = HlsStreamParams;
    type StreamCreationError = HlsStreamError;

    async fn create(params: Self::Params) -> Result<Self, Self::StreamCreationError> {
        Self::new(params.url, params.settings).await
    }

    fn content_length(&self) -> ContentLength {
        // HLS streams typically don't have a known content length
        ContentLength::Unknown
    }

    #[instrument(skip(self))]
    async fn seek_range(&mut self, start: u64, end: Option<u64>) -> io::Result<()> {
        trace!("HLS seek_range called: start={}, end={:?}", start, end);

        // Note: end parameter is ignored for HLS as we don't support bounded reads
        self.seek_internal(start)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    async fn reconnect(&mut self, current_position: u64) -> io::Result<()> {
        trace!("HLS reconnect called at position {}", current_position);

        // Try to seek to the current position
        self.seek_range(current_position, None).await
    }

    fn supports_seek(&self) -> bool {
        // HLS supports seeking through command system
        true
    }
}

impl Stream for HlsStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        match self.data_receiver.poll_recv(cx) {
            Poll::Ready(Some(data)) => {
                #[cfg(debug_assertions)]
                tracing::trace!("HlsStream::poll_next: returning {} bytes", data.len());
                Poll::Ready(Some(Ok(data)))
            }
            Poll::Ready(None) => {
                #[cfg(debug_assertions)]
                tracing::trace!("HlsStream::poll_next: channel closed");
                self.finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => {
                #[cfg(debug_assertions)]
                tracing::trace!("HlsStream::poll_next: no data available, pending");
                Poll::Pending
            }
        }
    }
}
