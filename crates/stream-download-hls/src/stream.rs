//! HLS implementation of the [`SourceStream`] trait.
//!
//! This module provides `HlsStream`, a `SourceStream` implementation for HLS streams.
//! It handles playlist parsing, segment downloading, and adaptive bitrate switching.
//!
//! The stream produces ordered [`StreamMsg`] items:
//! - `StreamMsg::Data(Bytes)` for payload data
//! - `StreamMsg::Control(StreamControl::...)` for ordered chunk boundaries and resources
//!
//! This ordering is the foundation for segmented storage (init/media segments in separate files)
//! while still exposing a single contiguous `Read + Seek` view via a stitched reader.
//!
//! In addition, this stream reports a **best-effort segmented content length** via
//! [`SourceStream::content_length`]. The segmented length snapshot is updated by the background
//! worker as it emits `ChunkStart/ChunkEnd` boundaries.
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

use crate::HlsStreamWorker;
use crate::error::HlsError;
use futures_util::Stream;
use stream_download::source::{SourceStream, StreamControl, StreamMsg};
use stream_download::storage::{ContentLength, SegmentedLength, StorageHandle};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, trace};
use url::Url;

/// Events emitted by the HLS streaming pipeline (out-of-band metadata).
#[derive(Clone, Debug)]
pub enum StreamEvent {
    /// ABR: a new variant (rendition) has been selected. Emitted before the init segment.
    VariantChanged {
        variant_id: crate::parser::VariantId,
        codec_info: Option<crate::parser::CodecInfo>,
    },
    /// Beginning of an init segment in the main byte stream.
    /// byte_len may be None when exact length is unknown (e.g., due to DRM/middleware).
    InitStart {
        variant_id: crate::parser::VariantId,
        codec_info: Option<crate::parser::CodecInfo>,
        byte_len: Option<u64>,
    },
    /// End of the init segment in the main byte stream.
    InitEnd {
        variant_id: crate::parser::VariantId,
    },
    /// Optional: media segment boundaries (useful for metrics).
    SegmentStart {
        sequence: u64,
        variant_id: crate::parser::VariantId,
        byte_len: Option<u64>,
        duration: std::time::Duration,
    },
    SegmentEnd {
        sequence: u64,
        variant_id: crate::parser::VariantId,
    },
}

/// Parameters for creating an HLS stream.
#[derive(Debug, Clone)]
pub struct HlsStreamParams {
    /// The URL of the HLS master playlist.
    pub url: Url,
    /// Unified settings for HLS playback and downloader behavior.
    pub settings: Arc<crate::HlsSettings>,
    /// Storage handle used for read-before-fetch caching of playlists/keys.
    ///
    /// This is mandatory to ensure caching is actually enabled (otherwise the cached downloader
    /// would be forced into "cache disabled" mode).
    pub storage_handle: StorageHandle,
}

impl HlsStreamParams {
    /// Create new HLS stream parameters.
    pub fn new(
        url: Url,
        settings: impl Into<Arc<crate::HlsSettings>>,
        storage_handle: StorageHandle,
    ) -> Self {
        Self {
            url,
            settings: settings.into(),
            storage_handle,
        }
    }
}

pub struct HlsStream {
    /// Channel receiver for ordered stream messages (data + control).
    data_receiver: mpsc::Receiver<StreamMsg>,
    /// Channel sender for seek commands to the streaming loop.
    seek_sender: mpsc::Sender<u64>,
    /// Cancellation token for graceful shutdown.
    cancel_token: CancellationToken,
    /// Task handle for background streaming.
    streaming_task: tokio::task::JoinHandle<()>,
    /// Event broadcaster for out-of-band stream events.
    event_sender: tokio::sync::broadcast::Sender<StreamEvent>,

    /// Best-effort snapshot of segmented content length, updated by the background worker.
    ///
    /// This allows `content_length()` to return `ContentLength::Segmented(...)` instead of
    /// `Unknown`, improving progress reporting and seek planning.
    segmented_length: Arc<std::sync::RwLock<SegmentedLength>>,
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
        url: Url,
        settings: Arc<crate::HlsSettings>,
        storage_handle: StorageHandle,
    ) -> Result<Self, HlsError> {
        Self::new_with_config(url, settings, storage_handle).await
    }

    /// Create a new HLS stream with custom downloader configuration.
    ///
    /// This is the primary constructor used by `SourceStream::create`.
    ///
    /// Internally, it now delegates to `new_with_worker(...)` by constructing a default
    /// `HlsStreamWorker` using `HlsStreamWorker::new(...)`.
    pub async fn new_with_config(
        url: Url,
        settings: Arc<crate::HlsSettings>,
        storage_handle: StorageHandle,
    ) -> Result<Self, HlsError> {
        // Create channels for data and commands
        // Use bounded channel for data to control backpressure with configurable buffer size
        let buffer_size = settings.prefetch_buffer_size;
        let (data_sender, data_receiver) = mpsc::channel::<StreamMsg>(buffer_size);
        let (seek_sender, seek_receiver) = mpsc::channel(1);
        let cancel_token = CancellationToken::new();

        // Create event broadcast channel (out-of-band metadata)
        let (event_sender, _event_receiver) = tokio::sync::broadcast::channel(64);

        // Best-effort segmented length snapshot shared with `content_length()`.
        let segmented_length = Arc::new(std::sync::RwLock::new(SegmentedLength::default()));

        // Stable-ish identifier used for persistent cache layout:
        // `<storage_root>/<master_hash>/<variant_id>/<segment_basename>`
        let master_hash = crate::master_hash_from_url(&url);

        // Construct the default worker (previous behavior).
        let worker = HlsStreamWorker::new(
            url,
            settings,
            storage_handle,
            data_sender,
            seek_receiver,
            cancel_token.clone(),
            event_sender.clone(),
            master_hash,
            segmented_length.clone(),
        )
        .await?;

        Self::new_with_worker(
            data_receiver,
            seek_sender,
            cancel_token,
            event_sender,
            segmented_length,
            worker,
        )
    }

    /// Create an HLS stream by supplying a fully constructed worker.
    ///
    /// This is useful for tests/fixtures where you want to:
    /// - provide a pre-built `HlsManager` / customized downloader,
    /// - inject a pre-configured `HlsStreamWorker`,
    /// - control initialization order deterministically.
    ///
    /// Important: the provided `worker` must be wired to the same `data_receiver`/`seek_sender`
    /// channels and cancellation token passed here.
    pub fn new_with_worker(
        data_receiver: mpsc::Receiver<StreamMsg>,
        seek_sender: mpsc::Sender<u64>,
        cancel_token: CancellationToken,
        event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
        segmented_length: Arc<std::sync::RwLock<SegmentedLength>>,
        worker: HlsStreamWorker,
    ) -> Result<Self, HlsError> {
        let streaming_task = tokio::spawn(async move {
            tracing::trace!("HLS streaming task started");
            let result = worker.run().await;

            if let Err(e) = result {
                match e {
                    HlsError::Cancelled => {
                        tracing::trace!("HLS streaming task cancelled")
                    }
                    _ => tracing::error!("HLS streaming loop error: {}", e),
                }
            }
            tracing::trace!("HLS streaming task finished");
        });

        Ok(Self {
            data_receiver,
            seek_sender,
            cancel_token,
            streaming_task,
            event_sender,
            segmented_length,
        })
    }

    /// Subscribe to out-of-band stream events (variant changes, init boundaries, etc).
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<StreamEvent> {
        self.event_sender.subscribe()
    }

    /// Seek to a specific position in the stream.
    ///
    /// Position is an absolute byte offset from the beginning of the concatenated stream.
    /// Supports forward and backward seeks. Backward seek will reset the internal controller
    /// state and replay from the start using byte-range where possible.
    #[inline(always)]
    async fn seek_internal(&self, position: u64) -> Result<(), HlsError> {
        self.seek_sender
            .send(position)
            .await
            .map_err(|_| HlsError::SeekFailed)?;
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
    type StreamCreationError = crate::error::HlsError;

    async fn create(params: Self::Params) -> Result<Self, Self::StreamCreationError> {
        Self::new(params.url, params.settings, params.storage_handle).await
    }

    fn content_length(&self) -> ContentLength {
        // Best-effort: if we have no segment boundaries yet, report Unknown to avoid a zero clamp
        // before the worker has a chance to populate lengths.
        match self.segmented_length.read() {
            Ok(guard) => {
                let has_lengths = guard
                    .segments
                    .iter()
                    .any(|seg| seg.reported > 0 || seg.gathered.is_some());
                if has_lengths {
                    ContentLength::Segmented(guard.clone())
                } else {
                    ContentLength::Unknown
                }
            }
            Err(_) => ContentLength::Unknown,
        }
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
    type Item = io::Result<StreamMsg>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        match self.data_receiver.poll_recv(cx) {
            Poll::Ready(Some(msg)) => {
                // Control messages are ordered relative to data and must be forwarded as-is.
                match &msg {
                    StreamMsg::Data(bytes) => {
                        tracing::trace!(
                            "HlsStream::poll_next: returning Data({} bytes)",
                            bytes.len()
                        );
                    }
                    StreamMsg::Control(StreamControl::ChunkStart { .. }) => {
                        tracing::trace!("HlsStream::poll_next: returning Control(ChunkStart)");
                    }
                    StreamMsg::Control(StreamControl::ChunkEnd { .. }) => {
                        tracing::trace!("HlsStream::poll_next: returning Control(ChunkEnd)");
                    }
                    StreamMsg::Control(StreamControl::StoreResource { .. }) => {
                        tracing::trace!("HlsStream::poll_next: returning Control(StoreResource)");
                    }
                    StreamMsg::Control(StreamControl::SetDefaultStreamKey { .. }) => {
                        tracing::trace!(
                            "HlsStream::poll_next: returning Control(SetDefaultStreamKey)"
                        );
                    }
                }
                Poll::Ready(Some(Ok(msg)))
            }
            Poll::Ready(None) => {
                tracing::trace!("HlsStream::poll_next: channel closed");
                Poll::Ready(None)
            }
            Poll::Pending => {
                tracing::trace!("HlsStream::poll_next: no data available, pending");
                Poll::Pending
            }
        }
    }
}
