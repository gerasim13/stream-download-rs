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

use crate::traits::NextSegmentResult;
use crate::{
    AbrConfig, AbrController, DownloaderConfig, HlsConfig, HlsManager, MediaStream,
    ResourceDownloader, SegmentType, SelectionMode,
};
use stream_download::source::{DecodeError, SourceStream};
use stream_download::storage::ContentLength;

/// Parameters for creating an HLS stream.
#[derive(Debug, Clone)]
pub struct HlsStreamParams {
    /// The URL of the HLS master playlist.
    pub url: Arc<str>,
    /// Configuration for HLS playback.
    pub hls_config: Arc<HlsConfig>,
    /// Configuration for adaptive bitrate control.
    pub abr_config: Arc<AbrConfig>,
    /// Selection mode for variants (auto or manual).
    pub selection_mode: SelectionMode,
    /// Optional configuration for the resource downloader.
    /// If not provided, uses optimized defaults.
    pub downloader_config: Option<DownloaderConfig>,
}

impl HlsStreamParams {
    /// Create new HLS stream parameters.
    pub fn new(
        url: impl Into<Arc<str>>,
        hls_config: impl Into<Arc<HlsConfig>>,
        abr_config: impl Into<Arc<AbrConfig>>,
        selection_mode: SelectionMode,
    ) -> Self {
        Self {
            url: url.into(),
            hls_config: hls_config.into(),
            abr_config: abr_config.into(),
            selection_mode,
            downloader_config: None,
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
        config: Arc<HlsConfig>,
        abr_config: Arc<AbrConfig>,
        selection_mode: SelectionMode,
    ) -> Result<Self, HlsStreamError> {
        Self::new_with_config(url, config, abr_config, selection_mode, None).await
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
        config: Arc<HlsConfig>,
        abr_config: Arc<AbrConfig>,
        selection_mode: SelectionMode,
        downloader_config: Option<DownloaderConfig>,
    ) -> Result<Self, HlsStreamError> {
        let url = url.into();

        // Create channels for data and commands
        // Use bounded channel for data to control backpressure with configurable buffer size
        let buffer_size = config.prefetch_buffer_size;
        let (data_sender, data_receiver) = mpsc::channel(buffer_size);
        let (seek_sender, seek_receiver) = mpsc::channel(1);
        let cancel_token = CancellationToken::new();

        // Start background streaming task
        let streaming_task = Self::start_streaming_task(
            url,
            config,
            abr_config,
            selection_mode,
            downloader_config,
            data_sender,
            seek_receiver,
            cancel_token.clone(),
        )
        .await?;

        Ok(Self {
            data_receiver,
            seek_sender,
            cancel_token,
            streaming_task,
            finished: false,
        })
    }

    /// Start the background streaming task.
    async fn start_streaming_task(
        url: Arc<str>,
        config: Arc<HlsConfig>,
        abr_config: Arc<AbrConfig>,
        selection_mode: SelectionMode,
        downloader_config: Option<DownloaderConfig>,
        data_sender: mpsc::Sender<Bytes>,
        seek_receiver: mpsc::Receiver<u64>,
        cancel_token: CancellationToken,
    ) -> Result<tokio::task::JoinHandle<()>, HlsStreamError> {
        let task = tokio::spawn(async move {
            tracing::trace!("HLS streaming task started");
            if let Err(e) = Self::streaming_loop(
                url,
                config,
                abr_config,
                selection_mode,
                downloader_config,
                data_sender,
                seek_receiver,
                cancel_token,
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
        config: Arc<HlsConfig>,
        abr_config: Arc<AbrConfig>,
        selection_mode: SelectionMode,
        downloader_config: Option<DownloaderConfig>,
        data_sender: mpsc::Sender<Bytes>,
        mut seek_receiver: mpsc::Receiver<u64>,
        cancel_token: CancellationToken,
    ) -> Result<(), HlsStreamError> {
        let downloader_config = downloader_config.unwrap_or_default();
        let downloader = ResourceDownloader::new(downloader_config);

        let mut manager = HlsManager::new(url, config, downloader);

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
            match selection_mode {
                SelectionMode::Auto => manager.current_variant_index().unwrap_or(0),
                SelectionMode::Manual(index) => index.0,
            }
        };

        let init_bw = master.variants[initial_variant_index]
            .bandwidth
            .unwrap_or(0) as f64;

        let mut controller = AbrController::new(
            manager,
            abr_config.as_ref().clone(),
            selection_mode,
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
                    tracing::warn!("HLS streaming loop: backward seek not supported");
                    return Err(HlsStreamError::Hls(HlsErrorKind::BackwardSeekNotSupported));
                }
                drop(reserved_permit);
                continue;
            }

            // Fetch next segment using non-blocking method
            let next_result = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    return Err(HlsStreamError::Cancelled);
                }
                seek = seek_receiver.recv() => {
                    if let Some(position) = seek {
                        tracing::trace!("HLS streaming loop: received seek to position {}", position);
                        if position >= current_position {
                            bytes_to_skip = position - current_position;
                        } else {
                            tracing::warn!("HLS streaming loop: backward seek not supported");
                            return Err(HlsStreamError::Hls(HlsErrorKind::BackwardSeekNotSupported));
                        }
                        drop(reserved_permit);
                        continue;
                    } else {
                        tracing::trace!("HLS stream: seek channel closed");
                        break;
                    }
                }
                result = controller.next_segment_nonblocking() => result,
            };

            match next_result {
                Ok(NextSegmentResult::Segment(segment_type)) => {
                    // Reset retry delay on success
                    retry_delay = std::time::Duration::from_millis(100);

                    let (data, is_init) = match segment_type {
                        SegmentType::Init(segment_data) => (segment_data.data, true),
                        SegmentType::Media(segment_data) => (segment_data.data, false),
                    };

                    if Self::send_data_with_skip(
                        data,
                        &mut bytes_to_skip,
                        &mut current_position,
                        &data_sender,
                        is_init,
                        Some(reserved_permit),
                    )
                    .await
                    .is_err()
                    {
                        tracing::trace!("HLS stream: receiver dropped, stopping");
                        break;
                    }
                }
                Ok(NextSegmentResult::EndOfStream) => {
                    tracing::trace!("HLS stream: end of stream");
                    break;
                }
                Ok(NextSegmentResult::NeedsRefresh { wait }) => {
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
                                    return Err(HlsStreamError::Hls(HlsErrorKind::BackwardSeekNotSupported));
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
                        "Failed to get next segment: {}, retrying in {:?}",
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
        Self::new_with_config(
            params.url,
            params.hls_config,
            params.abr_config,
            params.selection_mode,
            params.downloader_config,
        )
        .await
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
