#![allow(dead_code)]
//! HLS `AudioSource` implementation.
//!
//! This source wraps `stream-download-hls::HlsStream` and exposes an **ordered** stream of `SourceMsg`:
//! - `SourceMsg::Data(Bytes)` for payload bytes
//! - `SourceMsg::Control(SourceControl::...)` for ordered init/media boundaries
//! - `SourceMsg::EndOfStream` when upstream ends
//!
//! Design goals
//! ------------
//! 1) Deterministic, ordered boundaries for tests and the decode pipeline.
//!    We derive identities from in-band ordered `stream-download` controls (`StreamControl`), not
//!    from out-of-band broadcast events.
//!
//! 2) Manual variant switching (public API).
//!    `stream-download-audio` exposes `AudioCommand::SetHlsVariant`. This source listens on an
//!    optional command channel and calls `HlsStream::set_variant(VariantId)`.
//!
//! Implementation note
//! -------------------
//! We intentionally avoid `futures_util::stream::unfold` here because it requires an `FnMut` closure,
//! which makes capturing a `cmd_rx` receiver awkward (it would need to be moved into a closure that
//! can be called multiple times).
//!
//! Instead, we use `async_stream::stream!` and yield `io::Result<SourceMsg>` explicitly.
//! This avoids `?` inside branches that some macros may not accept and keeps control flow obvious.
//!
//! Variant/index derivation strategy
//! --------------------------------
//! We require the upstream ordered control protocol (`stream-download::source::StreamControl`) to
//! carry **neutral identifiers** on chunk boundaries:
//! - `variant: Option<u64>` — a stream/quality/variant identifier (HLS uses the master variant index)
//! - `index: Option<u64>` — a monotonically increasing chunk index (HLS uses the media sequence number)
//!
//! This keeps the core protocol stream-agnostic and allows strict ordered tests like:
//! - "segments are sequential with no gaps and no repeats (per variant)"
//! - "variant switches are applied at init/media boundaries"
//!
//! We intentionally do not rely on `SetDefaultStreamKey` ordering or broadcast events for identity.
//! `SetDefaultStreamKey` (if present) is treated as a no-op in this source.

use std::any::Any;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::Stream;
use futures_util::StreamExt;
use reqwest::Url;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use stream_download::source::{ChunkKind, StreamControl, StreamMsg};
use stream_download::storage::ProvidesStorageHandle;
use stream_download_hls::{HlsPersistentStorageProvider, HlsSettings, HlsStream, VariantId};

use super::{AudioSource, SourceControl, SourceMsg};
use crate::types::{AudioCommand, HlsChunkId};

/// HLS audio source backed by `stream-download-hls::HlsStream`.
pub struct HlsAudioSource {
    url: Url,
    hls_settings: HlsSettings,
    storage_root: Option<std::path::PathBuf>,
    cancel: CancellationToken,

    /// Optional command receiver for manual control (e.g. SetHlsVariant).
    ///
    /// This is set by the orchestration layer after downcasting the boxed source.
    pub(crate) cmd_rx: Option<mpsc::Receiver<AudioCommand>>,
}

impl HlsAudioSource {
    /// Create a new HLS audio source.
    pub fn new(
        url: Url,
        hls_settings: HlsSettings,
        storage_root: Option<std::path::PathBuf>,
    ) -> Self {
        Self {
            url,
            hls_settings,
            storage_root,
            cancel: CancellationToken::new(),
            cmd_rx: None,
        }
    }

    /// Inherent helper to attach a command receiver.
    ///
    /// This is intentionally an inherent method (not only a trait method) so the orchestration layer
    /// can inject commands after downcasting `Box<dyn AudioSource>` to `Box<HlsAudioSource>`.
    pub fn with_commands(mut self: Box<Self>, commands: mpsc::Receiver<AudioCommand>) -> Box<Self> {
        self.cmd_rx = Some(commands);
        self
    }
}

impl AudioSource for HlsAudioSource {
    fn name(&self) -> &'static str {
        "hls"
    }

    fn url(&self) -> &Url {
        &self.url
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn supports_commands(&self) -> bool {
        true
    }

    fn into_stream(self: Box<Self>) -> Pin<Box<dyn Stream<Item = io::Result<SourceMsg>> + Send>> {
        let url = self.url.clone();
        let hls_settings = self.hls_settings.clone();
        let storage_root = self.storage_root.clone();
        let cancel = self.cancel.clone();
        let mut cmd_rx = self.cmd_rx;

        let s = async_stream::stream! {
            // Build storage handle (persistent caching is required for HLS; also used by HLS crate).
            let root = storage_root
                .unwrap_or_else(|| std::env::temp_dir().join("stream-download-audio-hls"));

            if let Err(e) = std::fs::create_dir_all(&root) {
                yield Err(e);
                return;
            }

            let prefetch_bytes = std::num::NonZeroUsize::new(8 * 1024 * 1024).unwrap();
            let max_cached_streams = std::num::NonZeroUsize::new(10).unwrap();

            let provider = HlsPersistentStorageProvider::new_hls_file_tree(
                root,
                prefetch_bytes,
                Some(max_cached_streams),
            );

            let Some(storage_handle) = provider.storage_handle() else {
                yield Err(io::Error::new(
                    io::ErrorKind::Other,
                    "HLS persistent storage provider must vend a StorageHandle",
                ));
                return;
            };

            let mut stream = match HlsStream::new(url.clone(), Arc::new(hls_settings.clone()), storage_handle).await {
                Ok(s) => s,
                Err(e) => {
                    yield Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
                    return;
                }
            };

            let mut ctx = HlsControlContext::default();

            loop {
                if cancel.is_cancelled() {
                    yield Ok(SourceMsg::EndOfStream);
                    break;
                }

                // If commands are enabled, allow them to preempt reading.
                // Applied effects must be observed via ordered `StreamControl` boundaries emitted by `stream`.
                if let Some(rx) = cmd_rx.as_mut() {
                    tokio::select! {
                        biased;

                        cmd = rx.recv() => {
                            if let Some(cmd) = cmd {
                                match cmd {
                                    AudioCommand::SetHlsVariant { variant } => {
                                        let vid = VariantId(variant);
                                        if let Err(e) = stream.set_variant(vid).await {
                                            yield Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
                                            break;
                                        }
                                    }
                                }
                            } else {
                                // Commands channel closed; disable commands.
                                cmd_rx = None;
                            }
                            continue;
                        }

                        msg = stream.next() => {
                            match msg {
                                Some(Ok(StreamMsg::Data(b))) => {
                                    if !b.is_empty() {
                                        yield Ok(SourceMsg::Data(b));
                                    }
                                }
                                Some(Ok(StreamMsg::Control(c))) => {
                                    if let Some(out) = ctx.map_control(c) {
                                        yield Ok(out);
                                    }
                                }
                                Some(Err(e)) => {
                                    yield Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
                                    break;
                                }
                                None => {
                                    yield Ok(SourceMsg::EndOfStream);
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    // No command channel: just stream HLS messages.
                    match stream.next().await {
                        Some(Ok(StreamMsg::Data(b))) => {
                            if !b.is_empty() {
                                yield Ok(SourceMsg::Data(b));
                            }
                        }
                        Some(Ok(StreamMsg::Control(c))) => {
                            if let Some(out) = ctx.map_control(c) {
                                yield Ok(out);
                            }
                        }
                        Some(Err(e)) => {
                            yield Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
                            break;
                        }
                        None => {
                            yield Ok(SourceMsg::EndOfStream);
                            break;
                        }
                    }
                }
            }
        };

        Box::pin(s)
    }
}

/// Tracks ordered segmented identity as seen through ordered `StreamControl`.
///
/// This is intentionally stateless: we require the upstream to populate neutral `variant` and `index`
/// fields on `ChunkStart/ChunkEnd`. This avoids any reliance on `SetDefaultStreamKey` ordering.
#[derive(Default)]
struct HlsControlContext;

impl HlsControlContext {
    fn map_control(&mut self, ctrl: StreamControl) -> Option<SourceMsg> {
        match ctrl {
            // No-op: boundaries must be self-contained.
            StreamControl::SetDefaultStreamKey { .. } => None,

            StreamControl::ChunkStart {
                kind,
                variant,
                sequence,
                ..
            } => {
                let variant =
                    variant.expect("expected StreamControl::ChunkStart.variant to be set");
                let id = HlsChunkId {
                    variant: variant as usize,
                    sequence,
                };

                match kind {
                    ChunkKind::Init => Some(SourceMsg::Control(SourceControl::HlsInitStart { id })),
                    ChunkKind::Media => {
                        Some(SourceMsg::Control(SourceControl::HlsSegmentStart { id }))
                    }
                }
            }

            StreamControl::ChunkEnd {
                kind,
                variant,
                sequence,
                ..
            } => {
                let variant = variant.expect("expected StreamControl::ChunkEnd.variant to be set");
                let id = HlsChunkId {
                    variant: variant as usize,
                    sequence,
                };

                match kind {
                    ChunkKind::Init => Some(SourceMsg::Control(SourceControl::HlsInitEnd { id })),
                    ChunkKind::Media => {
                        Some(SourceMsg::Control(SourceControl::HlsSegmentEnd { id }))
                    }
                }
            }

            StreamControl::StoreResource { .. } => None,
        }
    }
}
