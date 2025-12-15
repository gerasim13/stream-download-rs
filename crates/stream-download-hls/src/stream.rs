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

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{self, Poll};

use bytes::Bytes;
use futures_util::Stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, trace};

use crate::{HlsStreamError, HlsStreamWorker, StreamEvent};
use stream_download::source::SourceStream;
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

pub struct HlsStream {
    /// Channel receiver for stream data.
    data_receiver: mpsc::Receiver<Bytes>,
    /// Channel sender for seek commands to the streaming loop.
    seek_sender: mpsc::Sender<u64>,
    /// Cancellation token for graceful shutdown.
    cancel_token: CancellationToken,
    /// Task handle for background streaming.
    streaming_task: tokio::task::JoinHandle<()>,
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
            let result = async {
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
            .await;

            if let Err(e) = result {
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
        match self.data_receiver.poll_recv(cx) {
            Poll::Ready(Some(data)) => {
                #[cfg(debug_assertions)]
                tracing::trace!("HlsStream::poll_next: returning {} bytes", data.len());
                Poll::Ready(Some(Ok(data)))
            }
            Poll::Ready(None) => {
                #[cfg(debug_assertions)]
                tracing::trace!("HlsStream::poll_next: channel closed");
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
