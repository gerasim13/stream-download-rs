use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use kanal::AsyncSender;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use url::Url;

use stream_download::source::{ChunkKind, SourceStream, StreamMsg};
use stream_download::storage::ProvidesStorageHandle;
use stream_download_hls::{HlsPersistentStorageProvider, HlsSettings, HlsStream, StreamEvent};

use crate::backends::ProducerCommand;
use crate::backends::common::io_other;
use crate::pipeline::Packet;

/// Internal state for HLS packet production.
struct HlsProducerState {
    /// Current init segment bytes (accumulated during init collection)
    current_init_bytes: Vec<u8>,
    /// Hash of current init segment (for detecting variant changes)
    current_init_hash: u64,
    /// Whether we're currently collecting init segment bytes
    is_collecting_init: bool,
    /// Media segment buffer (accumulated during media collection)
    media_buffer: Vec<u8>,
    /// Init bytes pending to be sent with next media packet (after init hash change)
    pending_init_bytes: Option<Bytes>,
    /// Current segment sequence number (from HLS events)
    current_sequence: Option<u64>,
    /// Current variant index (for future use)
    current_variant_index: Option<usize>,
}

impl HlsProducerState {
    fn new() -> Self {
        Self {
            current_init_bytes: Vec::new(),
            current_init_hash: 0,
            is_collecting_init: false,
            media_buffer: Vec::new(),
            pending_init_bytes: None,
            current_sequence: None,
            current_variant_index: None,
        }
    }

    /// Process all pending events from the broadcast channel.
    /// Updates sequence number from SegmentStart events.
    fn process_pending_events(
        &mut self,
        event_rx: &mut tokio::sync::broadcast::Receiver<StreamEvent>,
    ) {
        while let Ok(event) = event_rx.try_recv() {
            match event {
                StreamEvent::SegmentStart {
                    sequence,
                    variant_id,
                    ..
                } => {
                    tracing::trace!(
                        "HlsProducerState: segment started, sequence={}, variant={:?}",
                        sequence,
                        variant_id
                    );
                    self.current_sequence = Some(sequence);
                    self.current_variant_index = Some(variant_id.0);
                }
                _ => {
                    // Ignore other events for now
                }
            }
        }
    }

    /// Handle init segment end: compute hash and prepare for sending with next media.
    fn handle_init_end(&mut self) {
        self.is_collecting_init = false;

        // Compute hash of init segment
        let mut hasher = DefaultHasher::new();
        self.current_init_bytes.hash(&mut hasher);
        let new_hash = hasher.finish();

        tracing::trace!(
            "HlsProducerState: init segment ended, hash={}, size={}",
            new_hash,
            self.current_init_bytes.len()
        );

        // If hash changed, save init bytes to send with next media packet
        if new_hash != self.current_init_hash {
            tracing::trace!(
                "HlsProducerState: init hash changed: {} -> {}, saving init bytes to send with first media",
                self.current_init_hash,
                new_hash
            );
            self.current_init_hash = new_hash;
            self.pending_init_bytes = Some(Bytes::from(self.current_init_bytes.clone()));
        }
    }

    /// Build and return a packet from current media buffer.
    /// Returns None if media buffer is empty.
    fn build_packet(&mut self) -> Option<Packet> {
        if self.media_buffer.is_empty() {
            return None;
        }

        let init_bytes_to_send = self.pending_init_bytes.take().unwrap_or_else(Bytes::new);
        let pkt = Packet {
            init_hash: self.current_init_hash,
            init_bytes: init_bytes_to_send,
            media_bytes: Bytes::from(std::mem::take(&mut self.media_buffer)),
            variant_index: self.current_variant_index,
            sequence: self.current_sequence,
        };

        tracing::trace!(
            "HlsProducerState: built packet, init_bytes={}, media_bytes={}, sequence={:?}",
            pkt.init_bytes.len(),
            pkt.media_bytes.len(),
            pkt.sequence
        );

        Some(pkt)
    }

    /// Clear state after seek
    fn clear_after_seek(&mut self) {
        self.current_init_bytes.clear();
        self.media_buffer.clear();
        self.is_collecting_init = false;
        // Keep current_init_hash and current_sequence to maintain decoder state
    }
}

/// HLS packet producer implementation.
pub struct HlsPacketProducer {
    url: Url,
    settings: HlsSettings,
    storage_root: std::path::PathBuf,
}

impl HlsPacketProducer {
    /// Create a new HLS packet producer.
    ///
    /// # Arguments
    /// * `url` - HLS master playlist URL
    /// * `settings` - HLS-specific settings
    /// * `storage_root` - Optional storage root for persistent caching (defaults to temp dir)
    pub fn new(url: Url, settings: HlsSettings, storage_root: Option<std::path::PathBuf>) -> Self {
        let storage_root =
            storage_root.unwrap_or_else(|| std::env::temp_dir().join("stream-download-audio-hls"));

        Self {
            url,
            settings,
            storage_root,
        }
    }
}

#[async_trait]
impl crate::backends::PacketProducer for HlsPacketProducer {
    /// Async HLS packet producer for the unified pipeline.
    /// Emits self-contained Packet { init_hash, init_bytes, media_bytes } into `out`.
    ///
    /// This implementation directly reads HlsStream to intercept StreamMsg::Control messages,
    /// which allows us to:
    /// - Detect init segment boundaries (ChunkStart/ChunkEnd with kind=Init)
    /// - Collect init segment bytes
    /// - Compute init_hash for variant switching detection
    /// - Track variant_index for proper event reporting
    async fn run(
        &mut self,
        out: AsyncSender<Packet>,
        commands: Option<kanal::AsyncReceiver<crate::backends::ProducerCommand>>,
        cancel: Option<CancellationToken>,
    ) -> std::io::Result<()> {
        // Persistent storage provider + StorageHandle are now mandatory for HLS so that
        // read-before-fetch caching of playlists/keys is enabled.
        std::fs::create_dir_all(&self.storage_root)?;

        let prefetch_bytes = std::num::NonZeroUsize::new(8 * 1024 * 1024).unwrap();
        let max_cached_streams = std::num::NonZeroUsize::new(10).unwrap();

        let provider = HlsPersistentStorageProvider::new_hls_file_tree(
            self.storage_root.clone(),
            prefetch_bytes,
            Some(max_cached_streams),
        );

        let storage_handle = provider
            .storage_handle()
            .expect("HLS persistent storage provider must vend a StorageHandle");

        let mut stream = HlsStream::new(
            self.url.clone(),
            Arc::new(self.settings.clone()),
            storage_handle,
        )
        .await
        .map_err(|e| io_other(&e.to_string()))?;

        // Subscribe to HLS events to track segment sequence
        let mut event_rx = stream.subscribe_events();

        // Unified state management
        let mut state = HlsProducerState::new();

        // Main reading loop
        loop {
            // Check for cancellation
            if let Some(ref cancel_token) = cancel {
                if cancel_token.is_cancelled() {
                    tracing::trace!("HlsPacketProducer: cancelled");
                    return Ok(());
                }
            }

            // Process all pending events (updates sequence)
            state.process_pending_events(&mut event_rx);

            // Check for commands (non-blocking)
            if let Some(ref cmd_rx) = commands {
                match cmd_rx.try_recv() {
                    Ok(Some(ProducerCommand::Seek(position))) => {
                        tracing::trace!("HlsPacketProducer: seeking to {:?}", position);

                        // Convert Duration to byte position
                        // For HLS this is approximate - we seek to segment boundary
                        // TODO: More accurate seek using ContentLength::Segmented info
                        let byte_pos = (position.as_secs_f64() * 128_000.0) as u64; // Assume ~128KB/s

                        // Perform seek on HlsStream using SourceStream trait
                        if let Err(e) = stream.seek_range(byte_pos, None).await {
                            tracing::error!("HlsPacketProducer: seek failed: {}", e);
                            // Continue reading, don't fail
                        } else {
                            tracing::trace!(
                                "HlsPacketProducer: seek completed to byte pos {}",
                                byte_pos
                            );

                            // Clear buffers after seek
                            state.clear_after_seek();
                        }
                    }
                    Ok(Some(ProducerCommand::Flush)) => {
                        tracing::trace!("HlsPacketProducer: flushing buffers");
                        state.clear_after_seek();
                    }
                    Ok(None) | Err(_) => {
                        // No commands available or channel closed, continue
                    }
                }
            }

            // Read next message from HlsStream
            let msg = match stream.next().await {
                Some(Ok(msg)) => msg,
                Some(Err(e)) => {
                    tracing::error!("HlsPacketProducer: stream error: {}", e);
                    return Err(io_other(&e.to_string()));
                }
                None => {
                    tracing::trace!("HlsPacketProducer: stream ended");
                    // Send final packet if we have buffered media
                    if let Some(pkt) = state.build_packet() {
                        let _ = out.send(pkt).await;
                    }
                    return Ok(());
                }
            };

            match msg {
                StreamMsg::Data(bytes) => {
                    if state.is_collecting_init {
                        state.current_init_bytes.extend_from_slice(&bytes);
                    } else {
                        state.media_buffer.extend_from_slice(&bytes);
                    }
                }
                StreamMsg::Control(control) => {
                    use stream_download::source::StreamControl;
                    match control {
                        StreamControl::ChunkStart { kind, .. } => {
                            if kind == ChunkKind::Init {
                                tracing::trace!("HlsPacketProducer: init segment started");
                                state.is_collecting_init = true;
                                state.current_init_bytes.clear();
                            }
                        }
                        StreamControl::ChunkEnd { kind, .. } => {
                            // Before processing ChunkEnd, drain any pending events
                            state.process_pending_events(&mut event_rx);

                            if kind == ChunkKind::Init && state.is_collecting_init {
                                state.handle_init_end();
                            } else if kind == ChunkKind::Media {
                                // Media segment ended, build and send packet
                                if let Some(pkt) = state.build_packet() {
                                    if out.send(pkt).await.is_err() {
                                        tracing::error!(
                                            "HlsPacketProducer: failed to send packet - consumer dropped"
                                        );
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        _ => {
                            // Ignore other control messages for now
                        }
                    }
                }
            }
        }
    }
}
