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
        mut seek_receiver: mpsc::Receiver<u64>,
        cancel_token: CancellationToken,
        event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
    ) -> Result<(), HlsStreamError> {
        // Use flattened downloader settings from HlsSettings
        let (request_timeout, max_retries, retry_base_delay, max_retry_delay) = (
            settings.request_timeout,
            settings.max_retries,
            settings.retry_base_delay,
            settings.max_retry_delay,
        );
        // Keep reset clones for backward seek reinitialization
        let reset_url = url.clone();
        let reset_settings = settings.clone();
        // Use a dedicated streaming downloader built from flattened settings
        let streaming_downloader = ResourceDownloader::new(
            request_timeout,
            max_retries,
            retry_base_delay,
            max_retry_delay,
        );
        // Separate downloader instance for manager to avoid moving the streaming downloader
        let manager_downloader = ResourceDownloader::new(
            request_timeout,
            max_retries,
            retry_base_delay,
            max_retry_delay,
        );

        let mut manager = HlsManager::new(url, settings.clone(), manager_downloader);

        // Exponential backoff for retry logic
        let mut retry_delay = std::time::Duration::from_millis(100);
        const MAX_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

        manager
            .load_master()
            .await
            .map_err(|e| HlsStreamError::Hls(HlsErrorKind::MasterPlaylistLoad(e)))?;

        let master = manager
            .master()
            .ok_or_else(|| HlsStreamError::Hls(HlsErrorKind::StreamingLoopNotInitialized))?;

        // Choose initial variant
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

        // Track current byte position and bytes to skip
        let mut current_position: u64 = 0;
        let mut bytes_to_skip: u64 = 0;
        // Track current variant for event emission
        let mut last_variant_id: Option<crate::model::VariantId> = None;

        // Main streaming loop
        loop {
            // Wait for downstream capacity before fetching the next segment
            let reserved_permit = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    return Err(HlsStreamError::Cancelled);
                }
                permit = data_sender.reserve() => {
                    match permit {
                        Ok(p) => p,
                        Err(_) => {
                            tracing::trace!("HLS stream: receiver dropped before reserve");
                            break;
                        }
                    }
                }
            };

            // Check for seek commands first (non-blocking)
            if let Ok(position) = seek_receiver.try_recv() {
                tracing::trace!("HLS streaming loop: received seek to position {}", position);
                if position >= current_position {
                    bytes_to_skip = position - current_position;
                } else {
                    let (desc, intra) = controller
                        .inner_stream_mut()
                        .resolve_position(position)
                        .await
                        .map_err(|_| HlsStreamError::Hls(HlsErrorKind::SeekFailed))?;
                    controller
                        .inner_stream_mut()
                        .seek_to_descriptor(&desc)
                        .map_err(|_| HlsStreamError::Hls(HlsErrorKind::SeekFailed))?;
                    bytes_to_skip = intra;
                    current_position = position.saturating_sub(intra);
                }
                drop(reserved_permit);
                continue;
            }

            // Fetch next segment descriptor using non-blocking method
            let next_desc = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    return Err(HlsStreamError::Cancelled);
                }
                seek = seek_receiver.recv() => {
                    if let Some(position) = seek {
                        tracing::trace!("HLS streaming loop: received seek to position {}", position);
                        if position >= current_position {
                            // forward seek: skip bytes relative to current position
                            bytes_to_skip = position - current_position;
                        } else {
                            // precise backward seek using resolve_position
                            let (desc, intra) = controller
                                .inner_stream_mut()
                                .resolve_position(position)
                                .await
                                .map_err(|_| HlsStreamError::Hls(HlsErrorKind::SeekFailed))?;
                            controller
                                .inner_stream_mut()
                                .seek_to_descriptor(&desc)
                                .map_err(|_| HlsStreamError::Hls(HlsErrorKind::SeekFailed))?;
                            bytes_to_skip = intra;
                            current_position = position.saturating_sub(intra);
                        }
                        drop(reserved_permit);
                        continue;
                    } else {
                        tracing::trace!("HLS stream: seek channel closed");
                        break;
                    }
                }
                result = controller.inner_stream_mut().next_segment_descriptor_nonblocking() => result,
            };

            match next_desc {
                Ok(crate::manager::NextSegmentDescResult::Segment(desc)) => {
                    // Reset retry delay on success
                    retry_delay = std::time::Duration::from_millis(100);

                    // Determine segment size if needed for skip math
                    let seg_size_opt = if desc.is_init {
                        controller.inner_stream().segment_size(0)
                    } else {
                        controller.inner_stream().segment_size(desc.sequence)
                    };
                    let seg_size = if let Some(sz) = seg_size_opt {
                        Some(sz)
                    } else {
                        // Try probing if unknown
                        match controller
                            .inner_stream_mut()
                            .probe_and_record_segment_size(
                                if desc.is_init { 0 } else { desc.sequence },
                                &desc.uri,
                            )
                            .await
                        {
                            Ok(s) => s,
                            Err(_) => None, // best effort
                        }
                    };

                    // Handle full-segment skip if we have enough bytes_to_skip and know size
                    if let (Some(size), true) = (seg_size, bytes_to_skip > 0) {
                        if bytes_to_skip >= size {
                            bytes_to_skip -= size;
                            current_position += size;
                            drop(reserved_permit);
                            continue;
                        }
                    }

                    // Resolve DRM (AES-128-CBC) parameters before emitting events or building middlewares
                    let drm_params_opt = controller
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
                        .flatten();

                    let drm_applied = drm_params_opt.is_some();

                    // Emit events around init boundaries and variant changes
                    if desc.is_init {
                        // If variant changed, emit VariantChanged before InitStart
                        if last_variant_id != Some(desc.variant_id) {
                            let _ = event_sender.send(StreamEvent::VariantChanged {
                                variant_id: desc.variant_id,
                                codec_info: desc.codec_info.clone(),
                            });
                            last_variant_id = Some(desc.variant_id);
                        }
                        // For init, if DRM is applied the exact byte length after decryption is unknown -> None
                        let init_len_opt = if drm_applied {
                            None
                        } else {
                            controller.inner_stream().segment_size(0)
                        };
                        let _ = event_sender.send(StreamEvent::InitStart {
                            variant_id: desc.variant_id,
                            codec_info: desc.codec_info.clone(),
                            byte_len: init_len_opt,
                        });
                    } else {
                        // Media segment: if variant changed and there is no init, still notify
                        if last_variant_id != Some(desc.variant_id) {
                            let _ = event_sender.send(StreamEvent::VariantChanged {
                                variant_id: desc.variant_id,
                                codec_info: desc.codec_info.clone(),
                            });
                            last_variant_id = Some(desc.variant_id);
                        }
                        // Emit segment start with known length if available
                        let _ = event_sender.send(StreamEvent::SegmentStart {
                            sequence: desc.sequence,
                            variant_id: desc.variant_id,
                            byte_len: seg_size,
                            duration: desc.duration,
                        });
                    }

                    // Build a stream according to skip and init rules
                    let stream_res = if bytes_to_skip > 0 && !desc.is_init {
                        let start = bytes_to_skip;
                        bytes_to_skip = 0;
                        streaming_downloader
                            .stream_segment_range(&desc.uri, start, None)
                            .await
                    } else {
                        // For init segments, we always send the full segment even if skip is inside it
                        streaming_downloader.stream_segment(&desc.uri).await
                    };

                    let mut stream = match stream_res {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!(
                                "Failed to open segment stream: {}, retrying in {:?}",
                                e,
                                retry_delay
                            );
                            drop(reserved_permit);
                            tokio::select! {
                                biased;
                                _ = cancel_token.cancelled() => return Err(HlsStreamError::Cancelled),
                                _ = tokio::time::sleep(retry_delay) => {}
                            }
                            retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
                            continue;
                        }
                    };
                    // Apply streaming middlewares (DRM, etc.)
                    let mut middlewares: Vec<Arc<dyn StreamMiddleware>> = Vec::new();
                    if let Some((key, iv)) = drm_params_opt {
                        middlewares.push(Arc::new(Aes128CbcMiddleware::new(key, iv)));
                    }
                    let mut stream = apply_middlewares(stream, &middlewares);

                    // Send incoming chunks, interleaving cancellation and seek handling
                    let mut first_permit = Some(reserved_permit);
                    loop {
                        tokio::select! {
                            biased;
                            _ = cancel_token.cancelled() => {
                                return Err(HlsStreamError::Cancelled);
                            }
                            // Concurrently react to a seek during streaming a segment
                            seek = seek_receiver.recv() => {
                                if let Some(position) = seek {
                                    tracing::trace!("HLS streaming loop: received seek during streaming");
                                    if position >= current_position {
                                        bytes_to_skip = position - current_position;
                                    } else {
                                        let (desc, intra) = controller
                                            .inner_stream_mut()
                                            .resolve_position(position)
                                            .await
                                            .map_err(|_| HlsStreamError::Hls(HlsErrorKind::SeekFailed))?;
                                        controller
                                            .inner_stream_mut()
                                            .seek_to_descriptor(&desc)
                                            .map_err(|_| HlsStreamError::Hls(HlsErrorKind::SeekFailed))?;
                                        bytes_to_skip = intra;
                                        current_position = position.saturating_sub(intra);
                                        // break to apply new position on next iteration
                                    }
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
                                            None => match data_sender.reserve().await {
                                                Ok(p) => p,
                                                Err(_) => { tracing::trace!("HLS stream: receiver dropped, stopping"); break; }
                                            },
                                        };
                                        let len = chunk.len() as u64;
                                        permit.send(chunk);
                                        current_position = current_position.saturating_add(len);
                                    }
                                    Some(Err(e)) => {
                                        tracing::error!("Error reading segment chunk: {}, retrying in {:?}", e, retry_delay);
                                        tokio::select! {
                                            biased;
                                            _ = cancel_token.cancelled() => return Err(HlsStreamError::Cancelled),
                                            _ = tokio::time::sleep(retry_delay) => {}
                                        }
                                        retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
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

                    // Emit segment boundary events after finishing streaming
                    if desc.is_init {
                        let _ = event_sender.send(StreamEvent::InitEnd {
                            variant_id: desc.variant_id,
                        });
                    } else {
                        let _ = event_sender.send(StreamEvent::SegmentEnd {
                            sequence: desc.sequence,
                            variant_id: desc.variant_id,
                        });
                    }

                    // If we had a known segment size and current_position didn't advance fully (e.g., due to skipping inside init),
                    // ensure current_position accounts for the whole segment (to keep consistent semantics with previous implementation).
                    if let Some(size) = seg_size {
                        // No-op if we've already advanced >= size within this segment context
                        // This is best-effort; exact accounting happens via per-chunk increments above.
                        let _ = size;
                    }
                }
                Ok(crate::manager::NextSegmentDescResult::EndOfStream) => {
                    tracing::trace!("HLS stream: end of stream");
                    break;
                }
                Ok(crate::manager::NextSegmentDescResult::NeedsRefresh { wait }) => {
                    // Live stream needs to wait for new segments
                    // Use select! for proper cancellation during wait
                    drop(reserved_permit);

                    tokio::select! {
                        biased;
                        _ = cancel_token.cancelled() => {
                            return Err(HlsStreamError::Cancelled);
                        }
                        seek = seek_receiver.recv() => {
                            if let Some(position) = seek {
                                tracing::trace!("HLS streaming loop: received seek during wait");
                                if position >= current_position {
                                    bytes_to_skip = position - current_position;
                                } else {
                                    let (desc, intra) = controller
                                        .inner_stream_mut()
                                        .resolve_position(position)
                                        .await
                                        .map_err(|_| HlsStreamError::Hls(HlsErrorKind::SeekFailed))?;
                                    controller
                                        .inner_stream_mut()
                                        .seek_to_descriptor(&desc)
                                        .map_err(|_| HlsStreamError::Hls(HlsErrorKind::SeekFailed))?;
                                    bytes_to_skip = intra;
                                    current_position = position.saturating_sub(intra);
                                }
                            }
                            // Continue to next iteration regardless
                        }
                        _ = tokio::time::sleep(wait) => {
                            tracing::trace!("HLS stream: live refresh wait completed");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to get next segment descriptor: {}, retrying in {:?}",
                        e,
                        retry_delay
                    );
                    drop(reserved_permit);

                    // Wait with cancellation support
                    tokio::select! {
                        biased;
                        _ = cancel_token.cancelled() => {
                            return Err(HlsStreamError::Cancelled);
                        }
                        _ = tokio::time::sleep(retry_delay) => {}
                    }

                    // Exponential backoff with max limit
                    retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
                }
            }
        }

        Ok(())
    }

    /// Helper function to send data with skip logic.
    async fn send_data_with_skip(
        data: Bytes,
        bytes_to_skip: &mut u64,
        current_position: &mut u64,
        data_sender: &mpsc::Sender<Bytes>,
        is_init_segment: bool,
        reserved_permit: Option<mpsc::Permit<'_, Bytes>>,
    ) -> Result<(), ()> {
        let data_size = data.len() as u64;
        let mut reserved_permit = reserved_permit;

        // Handle skipping within data
        if *bytes_to_skip > 0 {
            if *bytes_to_skip >= data_size {
                // Skip entire data
                *bytes_to_skip -= data_size;
                *current_position += data_size;
                return Ok(());
            } else {
                // Skip part of data
                // For init segment, we must send it completely if skip is inside it
                if is_init_segment {
                    // Send full init segment (cannot send partially)
                    let permit = match reserved_permit.take() {
                        Some(p) => p,
                        None => data_sender.reserve().await.map_err(|_| ())?,
                    };
                    permit.send(data);
                    *current_position += data_size;
                    *bytes_to_skip = 0;
                    return Ok(());
                } else {
                    // For media segments, skip part and send remaining
                    let skip_bytes = *bytes_to_skip as usize;
                    let remaining_data = if skip_bytes < data.len() {
                        data.slice(skip_bytes..)
                    } else {
                        Bytes::new()
                    };

                    if !remaining_data.is_empty() {
                        let permit = match reserved_permit.take() {
                            Some(p) => p,
                            None => data_sender.reserve().await.map_err(|_| ())?,
                        };
                        permit.send(remaining_data);
                    }

                    *current_position += data_size;
                    *bytes_to_skip = 0;
                    return Ok(());
                }
            }
        }

        // Send full data
        let permit = match reserved_permit {
            Some(p) => p,
            None => data_sender.reserve().await.map_err(|_| ())?,
        };
        permit.send(data);
        *current_position += data_size;
        Ok(())
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
