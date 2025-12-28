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
use std::sync::{Arc, RwLock};
use std::task::{self, Poll};

use crate::HlsStreamWorker;
use crate::error::HlsError;
use crate::parser::{CodecInfo, VariantId};
use crate::settings::HlsSettings;

use futures_util::Stream;
use stream_download::source::{SourceStream, StreamMsg};
use stream_download::storage::{ContentLength, SegmentedLength, StorageHandle};
use tokio::sync::{broadcast, mpsc};
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
    data_rx: mpsc::Receiver<StreamMsg>,
    /// Sender for unified commands to the worker loop.
    cmd_tx: mpsc::Sender<HlsCommand>,
    /// Broadcaster for out-of-band stream events.
    event_tx: broadcast::Sender<StreamEvent>,
    /// Background worker task handle.
    streaming_task: tokio::task::JoinHandle<()>,
    /// Cancellation token for shutdown.
    cancel_token: CancellationToken,
    /// Best-effort segmented length snapshot used by `content_length()`.
    segmented_length: Arc<RwLock<SegmentedLength>>,
}

impl HlsStream {
    /// Creates a new HLS stream and spawns a default [`HlsStreamWorker`].
    pub async fn new(
        url: Url,
        settings: Arc<HlsSettings>,
        storage_handle: StorageHandle,
    ) -> Result<Self, HlsError> {
        // Create channels for data and commands (bounded data channel provides backpressure).
        let buffer_size = settings.prefetch_buffer_size;
        let (data_tx, data_rx) = mpsc::channel::<StreamMsg>(buffer_size);
        let (cmd_tx, cmd_rx) = mpsc::channel::<HlsCommand>(8);
        let (event_tx, _event_rx) = broadcast::channel(64);
        let cancel_token = CancellationToken::new();
        // Best-effort segmented length snapshot shared with `content_length()`.
        let segmented_length = Arc::new(RwLock::new(SegmentedLength::default()));

        // Construct the default worker (previous behavior).
        let worker = HlsStreamWorker::new(
            url,
            settings,
            storage_handle,
            data_tx,
            cmd_rx,
            cancel_token.clone(),
            event_tx.clone(),
            segmented_length.clone(),
        )
        .await?;

        // Spawn the worker task.
        let streaming_task = tokio::spawn(async move {
            tracing::trace!("HLS streaming task started");
            match worker.run().await {
                Ok(_) => tracing::trace!("HLS streaming task finished"),
                Err(HlsError::Cancelled) => tracing::trace!("HLS streaming task cancelled"),
                Err(e) => tracing::error!("HLS streaming loop error: {}", e),
            }
        });

        Ok(Self {
            data_rx,
            cmd_tx,
            cancel_token,
            streaming_task,
            event_tx,
            segmented_length,
        })
    }

    /// Subscribes to out-of-band stream events.
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<StreamEvent> {
        self.event_tx.subscribe()
    }

    /// Returns a clone of the unified command sender used to control the worker.
    pub fn command_sender(&self) -> mpsc::Sender<HlsCommand> {
        self.cmd_tx.clone()
    }

    /// Sends a unified command to the worker.
    #[inline(always)]
    async fn send_cmd(&self, cmd: HlsCommand) -> Result<(), HlsError> {
        self.cmd_tx
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
    #[inline(always)]
    pub async fn set_variant(&self, variant_id: VariantId) -> Result<(), HlsError> {
        self.send_cmd(HlsCommand::SetVariant { variant_id }).await
    }

    /// Clears manual variant selection and returns to AUTO (ABR-controlled) selection.
    #[inline(always)]
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
        match self.data_rx.poll_recv(cx) {
            Poll::Ready(Some(msg)) => Poll::Ready(Some(Ok(msg))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
