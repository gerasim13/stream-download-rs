//! HLS [`SourceStream`] implementation.
//!
//! [`HlsStream`] spawns a background [`HlsStreamWorker`] and exposes ordered [`StreamMsg`] items
//! (data + control). Chunk boundaries are emitted as `StreamControl` messages.
//!
//! This stream also exposes a **command channel** for runtime control:
//! - seek (byte position)
//! - manual variant selection (by `VariantId`)
//!
//! Detailed architecture notes live in `crates/stream-download-hls/README.md`.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{self, Poll};

use crate::HlsStreamWorker;
use crate::cache::keys::master_hash_from_url;
use crate::error::HlsError;
use crate::parser::{CodecInfo, VariantId};
use crate::settings::HlsSettings;

use futures_util::Stream;
use stream_download::source::{SourceStream, StreamControl, StreamMsg};
use stream_download::storage::{ContentLength, SegmentedLength, StorageHandle};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, trace};
use url::Url;

/// Out-of-band events emitted by the HLS streaming pipeline.
#[derive(Clone, Debug)]
pub enum StreamEvent {
    /// Variant changed (emitted before init/media boundaries for the new variant).
    VariantChanged {
        variant_id: VariantId,
        codec_info: Option<CodecInfo>,
    },
    /// Init segment start.
    /// `byte_len` may be `None` when the size is unknown.
    InitStart {
        variant_id: VariantId,
        codec_info: Option<CodecInfo>,
        byte_len: Option<u64>,
    },
    /// Init segment end.
    InitEnd { variant_id: VariantId },
    /// Media segment start.
    SegmentStart {
        sequence: u64,
        variant_id: VariantId,
        byte_len: Option<u64>,
        duration: std::time::Duration,
    },
    /// Media segment end.
    SegmentEnd {
        sequence: u64,
        variant_id: VariantId,
    },
}

/// Runtime control commands for [`HlsStream`].
///
/// This is a unified command channel so callers can control playback without
/// reaching into worker internals.
///
/// Notes:
/// - Commands are best-effort: if the worker has shut down, the send fails.
/// - Ordering: commands are not ordered relative to `StreamMsg` items; consumers
///   should observe applied effects via ordered boundaries (`StreamControl`) and/or
///   out-of-band `StreamEvent` (best-effort).
#[derive(Clone, Debug)]
pub enum HlsCommand {
    /// Seek to an absolute byte position in the concatenated logical stream.
    Seek { position: u64 },

    /// Manually select a variant. This is intended for "manual mode".
    ///
    /// The worker/controller should switch to the requested variant and continue streaming
    /// from a time-aligned segment boundary.
    SetVariant { variant_id: VariantId },

    /// Clear manual selection and return to AUTO (ABR-controlled) selection.
    ClearVariantOverride,
}

/// Parameters for creating an [`HlsStream`].
#[derive(Debug, Clone)]
pub struct HlsStreamParams {
    /// The URL of the HLS master playlist.
    pub url: Url,
    /// Unified settings for HLS playback and downloader behavior.
    pub settings: Arc<HlsSettings>,
    /// Storage handle used for read-before-fetch caching of playlists/keys.
    pub storage_handle: StorageHandle,
}

impl HlsStreamParams {
    /// Creates stream parameters.
    pub fn new(
        url: Url,
        settings: impl Into<Arc<HlsSettings>>,
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
    /// Receiver for ordered stream messages (data + control).
    data_receiver: mpsc::Receiver<StreamMsg>,
    /// Sender for unified commands to the worker loop.
    cmd_sender: mpsc::Sender<HlsCommand>,
    /// Cancellation token for shutdown.
    cancel_token: CancellationToken,
    /// Background worker task handle.
    streaming_task: tokio::task::JoinHandle<()>,
    /// Broadcaster for out-of-band stream events.
    event_sender: tokio::sync::broadcast::Sender<StreamEvent>,

    /// Best-effort segmented length snapshot used by `content_length()`.
    segmented_length: Arc<std::sync::RwLock<SegmentedLength>>,
}

impl HlsStream {
    /// Creates a new HLS stream (convenience wrapper around [`Self::new_with_config`]).
    pub async fn new(
        url: Url,
        settings: Arc<crate::HlsSettings>,
        storage_handle: StorageHandle,
    ) -> Result<Self, HlsError> {
        Self::new_with_config(url, settings, storage_handle).await
    }

    /// Creates a new HLS stream and spawns a default [`HlsStreamWorker`].
    ///
    /// This is the constructor used by `SourceStream::create`.
    pub async fn new_with_config(
        url: Url,
        settings: Arc<HlsSettings>,
        storage_handle: StorageHandle,
    ) -> Result<Self, HlsError> {
        // Create channels for data and commands (bounded data channel provides backpressure).
        let buffer_size = settings.prefetch_buffer_size;
        let (data_sender, data_receiver) = mpsc::channel::<StreamMsg>(buffer_size);
        let (cmd_sender, cmd_receiver) = mpsc::channel::<HlsCommand>(8);
        let cancel_token = CancellationToken::new();

        // Event broadcast channel (out-of-band metadata).
        let (event_sender, _event_receiver) = tokio::sync::broadcast::channel(64);

        // Best-effort segmented length snapshot shared with `content_length()`.
        let segmented_length = Arc::new(std::sync::RwLock::new(SegmentedLength::default()));

        // Identifier used for persistent cache layout:
        // `<storage_root>/<master_hash>/<variant_id>/<segment_basename>`
        let master_hash = master_hash_from_url(&url);

        // Construct the default worker (previous behavior).
        let worker = HlsStreamWorker::new(
            url,
            settings,
            storage_handle,
            data_sender,
            cmd_receiver,
            cancel_token.clone(),
            event_sender.clone(),
            master_hash,
            segmented_length.clone(),
        )
        .await?;

        Self::new_with_worker(
            data_receiver,
            cmd_sender,
            cancel_token,
            event_sender,
            segmented_length,
            worker,
        )
    }

    /// Creates an HLS stream by supplying a fully constructed worker.
    ///
    /// The provided `worker` must be wired to the channels and cancellation token passed here.
    pub fn new_with_worker(
        data_receiver: mpsc::Receiver<StreamMsg>,
        cmd_sender: mpsc::Sender<HlsCommand>,
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
            cmd_sender,
            cancel_token,
            streaming_task,
            event_sender,
            segmented_length,
        })
    }

    /// Subscribes to out-of-band stream events.
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<StreamEvent> {
        self.event_sender.subscribe()
    }

    /// Sends a unified command to the worker.
    #[inline(always)]
    async fn send_cmd(&self, cmd: HlsCommand) -> Result<(), HlsError> {
        self.cmd_sender
            .send(cmd)
            .await
            .map_err(|_| HlsError::SeekFailed)?;
        Ok(())
    }

    /// Seeks to an absolute byte offset in the concatenated stream.
    #[inline(always)]
    async fn seek_internal(&self, position: u64) -> Result<(), HlsError> {
        self.send_cmd(HlsCommand::Seek { position }).await
    }

    /// Manually selects a variant.
    pub async fn set_variant(&self, variant_id: VariantId) -> Result<(), HlsError> {
        self.send_cmd(HlsCommand::SetVariant { variant_id }).await
    }

    /// Clears manual variant selection and returns to AUTO (ABR-controlled) selection.
    pub async fn clear_variant_override(&self) -> Result<(), HlsError> {
        self.send_cmd(HlsCommand::ClearVariantOverride).await
    }
}

impl Drop for HlsStream {
    fn drop(&mut self) {
        // Cancel the streaming task.
        self.cancel_token.cancel();

        // Abort the task if it's still running.
        if !self.streaming_task.is_finished() {
            self.streaming_task.abort();
        }
    }
}

impl SourceStream for HlsStream {
    type Params = HlsStreamParams;
    type StreamCreationError = HlsError;

    async fn create(params: Self::Params) -> Result<Self, Self::StreamCreationError> {
        Self::new(params.url, params.settings, params.storage_handle).await
    }

    fn content_length(&self) -> ContentLength {
        // Best-effort: report `Unknown` until the worker observes at least one segment boundary.
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
