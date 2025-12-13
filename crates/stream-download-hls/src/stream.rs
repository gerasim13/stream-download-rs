//! HLS implementation of the [`SourceStream`] trait.
//!
//! This module provides `HlsStream`, a `SourceStream` implementation for HLS streams.
//! It handles playlist parsing, segment downloading, and adaptive bitrate switching.
//!
//! The stream produces raw bytes from HLS media segments, including init segments
//! when available. The stream concatenates init segment data with media segment data
//! to ensure proper decoding.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::pin::Pin;
use std::task::{self, Poll};

use bytes::Bytes;
use futures_util::{Future, Stream};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tracing::{instrument, trace};

use crate::{
    AbrConfig, AbrController, DownloaderConfig, HlsConfig, HlsManager, MediaStream,
    ResourceDownloader, SelectionMode,
};
use stream_download::source::{DecodeError, SourceStream};
use stream_download::storage::ContentLength;

/// Parameters for creating an HLS stream.
#[derive(Debug, Clone)]
pub struct HlsStreamParams {
    /// The URL of the HLS master playlist.
    pub url: String,
    /// Configuration for HLS playback.
    pub hls_config: HlsConfig,
    /// Configuration for adaptive bitrate control.
    pub abr_config: AbrConfig,
    /// Selection mode for variants (auto or manual).
    pub selection_mode: SelectionMode,
}

impl HlsStreamParams {
    /// Create new HLS stream parameters.
    pub fn new(
        url: impl Into<String>,
        hls_config: HlsConfig,
        abr_config: AbrConfig,
        selection_mode: SelectionMode,
    ) -> Self {
        Self {
            url: url.into(),
            hls_config,
            abr_config,
            selection_mode,
        }
    }
}

/// Error type for HLS stream creation and operations.
#[derive(Debug)]
pub enum HlsStreamError {
    /// IO error occurred.
    Io(String),
    /// HLS-specific error occurred.
    Hls(String),
    /// Invalid parameters provided.
    InvalidParams(String),
}

impl Display for HlsStreamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HlsStreamError::Io(msg) => write!(f, "IO error: {}", msg),
            HlsStreamError::Hls(msg) => write!(f, "HLS error: {}", msg),
            HlsStreamError::InvalidParams(msg) => write!(f, "Invalid parameters: {}", msg),
        }
    }
}

impl Error for HlsStreamError {}

impl DecodeError for HlsStreamError {
    async fn decode_error(self) -> String {
        self.to_string()
    }
}

/// Commands that can be sent to the streaming loop.
enum StreamCommand {
    /// Seek to a specific byte position.
    Seek(u64),
}

/// Internal state of the HLS stream.
struct HlsStreamState {
    /// Whether the stream has finished (end of VOD stream reached).
    finished: bool,
    /// Channel receiver for stream data.
    data_receiver: Option<mpsc::Receiver<Bytes>>,
    /// Channel sender for commands to the streaming loop.
    command_sender: Option<mpsc::UnboundedSender<StreamCommand>>,
    /// Task handle for background streaming.
    streaming_task: Option<tokio::task::JoinHandle<()>>,
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
    /// Internal state protected by a mutex for async access.
    state: Mutex<HlsStreamState>,
    /// The URL of the master playlist.
    url: String,
    /// HLS configuration.
    config: HlsConfig,
    /// ABR configuration.
    abr_config: AbrConfig,
    /// Selection mode.
    selection_mode: SelectionMode,
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
        url: impl Into<String>,
        config: HlsConfig,
        abr_config: AbrConfig,
        selection_mode: SelectionMode,
    ) -> Result<Self, HlsStreamError> {
        let url = url.into();

        // Create channels for data and commands
        // Use bounded channel for data to control backpressure (4 segments buffer)
        let (data_sender, data_receiver) = mpsc::channel(4);
        let (command_sender, command_receiver) = mpsc::unbounded_channel();

        let state = HlsStreamState {
            finished: false,
            data_receiver: Some(data_receiver),
            command_sender: Some(command_sender),
            streaming_task: None,
        };

        let stream = Self {
            state: Mutex::new(state),
            url,
            config,
            abr_config,
            selection_mode,
        };

        // Start background streaming task
        stream
            .start_streaming_task(data_sender, command_receiver)
            .await?;

        Ok(stream)
    }

    /// Start the background streaming task.
    async fn start_streaming_task(
        &self,
        data_sender: mpsc::Sender<Bytes>,
        command_receiver: mpsc::UnboundedReceiver<StreamCommand>,
    ) -> Result<(), HlsStreamError> {
        let mut state = self.state.lock().await;

        if state.streaming_task.is_some() {
            // Task already running
            return Ok(());
        }

        // Clone necessary data for the background task
        let url = self.url.clone();
        let config = self.config.clone();
        let abr_config = self.abr_config.clone();
        let selection_mode = self.selection_mode;

        let task = tokio::spawn(async move {
            tracing::trace!("HLS streaming task started");
            if let Err(e) = Self::streaming_loop(
                url,
                config,
                abr_config,
                selection_mode,
                data_sender,
                command_receiver,
            )
            .await
            {
                tracing::error!("HLS streaming loop error: {}", e);
            }
            tracing::trace!("HLS streaming task finished");
        });

        state.streaming_task = Some(task);
        Ok(())
    }

    /// Main streaming loop that runs in a background task.
    async fn streaming_loop(
        url: String,
        config: HlsConfig,
        abr_config: AbrConfig,
        selection_mode: SelectionMode,
        data_sender: mpsc::Sender<Bytes>,
        mut command_receiver: mpsc::UnboundedReceiver<StreamCommand>,
    ) -> Result<(), HlsStreamError> {
        let downloader = ResourceDownloader::new(DownloaderConfig::default());
        let mut manager = HlsManager::new(url.clone(), config.clone(), downloader);

        if let Err(e) = manager.load_master().await {
            return Err(HlsStreamError::Hls(format!(
                "Failed to load master playlist: {}",
                e
            )));
        }

        let master = manager.master().ok_or_else(|| {
            HlsStreamError::Hls("Master playlist not available after load_master()".to_string())
        })?;

        // Choose initial variant (simplified - always first variant for now)
        let initial_variant_index = if master.variants.is_empty() {
            return Err(HlsStreamError::Hls(
                "No variants available in master playlist".to_string(),
            ));
        } else {
            0
        };

        let init_bw = master.variants[initial_variant_index]
            .bandwidth
            .unwrap_or(0) as f64;

        let mut controller = AbrController::new(
            manager,
            abr_config.clone(),
            selection_mode,
            initial_variant_index,
            init_bw,
        );

        if let Err(e) = controller.init().await {
            return Err(HlsStreamError::Hls(format!(
                "Failed to initialize ABR controller: {}",
                e
            )));
        }

        // Track current byte position and bytes to skip
        let mut current_position: u64 = 0;
        let mut bytes_to_skip: u64 = 0;
        let mut need_init_segment = true;

        // Helper function to send data with skip logic
        async fn send_data_with_skip(
            data: Bytes,
            bytes_to_skip: &mut u64,
            current_position: &mut u64,
            data_sender: &mpsc::Sender<Bytes>,
            is_init_segment: bool,
        ) -> Result<(), ()> {
            let data_size = data.len() as u64;

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
                        if let Err(_) = data_sender.send(data).await {
                            return Err(());
                        }
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

                        if !remaining_data.is_empty()
                            && let Err(_) = data_sender.send(remaining_data).await
                        {
                            return Err(());
                        }

                        *current_position += data_size;
                        *bytes_to_skip = 0;
                        return Ok(());
                    }
                }
            }

            // Send full data
            if let Err(_) = data_sender.send(data).await {
                return Err(());
            }
            *current_position += data_size;
            Ok(())
        }

        // Main streaming loop
        loop {
            // Check for commands non-blockingly
            while let Ok(command) = command_receiver.try_recv() {
                match command {
                    StreamCommand::Seek(position) => {
                        tracing::trace!(
                            "HLS streaming loop: received seek to position {}",
                            position
                        );

                        if position >= current_position {
                            // Seeking forward - skip bytes
                            bytes_to_skip = position - current_position;
                        } else {
                            // Seeking backward - restart from beginning
                            // For now, we only support seeking forward or to current position
                            // In a full implementation, we would need to restart the controller
                            tracing::warn!(
                                "HLS streaming loop: seeking backward not fully supported, resetting to beginning"
                            );
                            bytes_to_skip = position;
                            current_position = 0;
                            need_init_segment = true;

                            // Reset controller to start from beginning
                            // This is simplified - in real implementation we would need to
                            // reinitialize the controller with the new position
                        }
                    }
                }
            }

            // Send init segment if needed
            if need_init_segment {
                // We'll get init segment from next_segment() now
                // Set flag to false to avoid infinite loop
                need_init_segment = false;
            }

            // Get next segment
            match controller.next_segment().await {
                Ok(Some(segment_type)) => {
                    match segment_type {
                        crate::traits::SegmentType::Init(segment_data) => {
                            // Send init segment with skip logic
                            // is_init_segment = true ensures init segment is sent completely if skip is inside it
                            if let Err(_) = send_data_with_skip(
                                segment_data.data,
                                &mut bytes_to_skip,
                                &mut current_position,
                                &data_sender,
                                true, // is_init_segment
                            )
                            .await
                            {
                                tracing::trace!("HLS stream: receiver dropped during init segment");
                                break;
                            }
                        }
                        crate::traits::SegmentType::Media(segment_data) => {
                            if let Err(_) = send_data_with_skip(
                                segment_data.data,
                                &mut bytes_to_skip,
                                &mut current_position,
                                &data_sender,
                                false, // is_init_segment
                            )
                            .await
                            {
                                tracing::trace!("HLS stream: receiver dropped, stopping");
                                break;
                            }
                        }
                    }
                }
                Ok(None) => {
                    // End of VOD stream reached
                    tracing::trace!("HLS stream: end of VOD stream, refreshing playlist");

                    // Try to refresh the media playlist
                    if let Err(e) = controller.inner_stream_mut().refresh_media_playlist().await {
                        tracing::error!("Failed to refresh media playlist: {}", e);
                    }

                    // Sleep a bit before checking again
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }
                Err(e) => {
                    tracing::error!("Failed to get next segment: {}", e);

                    // Sleep before retrying to avoid tight loop
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    continue;
                }
            }
        }

        Ok(())
    }

    /// Seek to a specific position in the stream.
    async fn seek_internal(&self, position: u64) -> Result<(), HlsStreamError> {
        let state = self.state.lock().await;

        // Send seek command to the streaming loop
        if let Some(command_sender) = &state.command_sender {
            if command_sender.send(StreamCommand::Seek(position)).is_err() {
                return Err(HlsStreamError::Hls(
                    "Failed to send seek command to streaming loop".to_string(),
                ));
            }
            Ok(())
        } else {
            Err(HlsStreamError::Hls(
                "Streaming loop not initialized".to_string(),
            ))
        }
    }
}

impl SourceStream for HlsStream {
    type Params = HlsStreamParams;
    type StreamCreationError = HlsStreamError;

    async fn create(params: Self::Params) -> Result<Self, Self::StreamCreationError> {
        Self::new(
            params.url,
            params.hls_config,
            params.abr_config,
            params.selection_mode,
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(_) => {
                // Lock is held by another task, try again later
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        };

        if let Some(receiver) = &mut state.data_receiver {
            match receiver.poll_recv(cx) {
                Poll::Ready(Some(data)) => {
                    tracing::trace!("HlsStream::poll_next: returning {} bytes", data.len());
                    Poll::Ready(Some(Ok(data)))
                }
                Poll::Ready(None) => {
                    tracing::trace!("HlsStream::poll_next: channel closed");
                    // Channel closed
                    state.data_receiver = None;
                    state.finished = true;
                    Poll::Ready(None)
                }
                Poll::Pending => {
                    tracing::trace!("HlsStream::poll_next: no data available, pending");
                    // No data available yet
                    Poll::Pending
                }
            }
        } else {
            tracing::trace!("HlsStream::poll_next: no receiver, stream finished");
            // No receiver, stream finished
            Poll::Ready(None)
        }
    }
}

/// Extension trait for creating HLS streams.
pub trait HlsStreamExt {
    /// Create a new HLS stream with the given parameters.
    fn new_hls(
        params: HlsStreamParams,
    ) -> impl Future<Output = Result<HlsStream, HlsStreamError>> + Send;
}

impl HlsStreamExt for HlsStream {
    fn new_hls(
        params: HlsStreamParams,
    ) -> impl Future<Output = Result<HlsStream, HlsStreamError>> + Send {
        Self::create(params)
    }
}
