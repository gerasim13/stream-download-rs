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
use stream_download_hls::{HlsPersistentStorageProvider, HlsSettings, HlsStream};

use crate::backends::common::io_other;
use crate::backends::ProducerCommand;
use crate::pipeline::Packet;

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
        let storage_root = storage_root.unwrap_or_else(|| {
            std::env::temp_dir().join("stream-download-audio-hls")
        });
        
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

        let mut stream = HlsStream::new(self.url.clone(), Arc::new(self.settings.clone()), storage_handle)
            .await
            .map_err(|e| io_other(&e.to_string()))?;

        // State for tracking init segments
        let mut current_init_bytes = Vec::new();
        let mut current_init_hash: u64 = 0;
        let mut is_collecting_init = false;
        let mut media_buffer = Vec::new();

        // Main reading loop
        loop {
            // Check for cancellation
            if let Some(ref cancel_token) = cancel {
                if cancel_token.is_cancelled() {
                    tracing::trace!("HlsPacketProducer: cancelled");
                    return Ok(());
                }
            }

            // Check for commands (non-blocking)
            if let Some(ref cmd_rx) = commands {
                match cmd_rx.try_recv() {
                    Ok(Some(ProducerCommand::Seek(position))) => {
                        tracing::info!("HlsPacketProducer: seeking to {:?}", position);
                        
                        // Convert Duration to byte position
                        // For HLS this is approximate - we seek to segment boundary
                        // TODO: More accurate seek using ContentLength::Segmented info
                        let byte_pos = (position.as_secs_f64() * 128_000.0) as u64; // Assume ~128KB/s
                        
                        // Perform seek on HlsStream using SourceStream trait
                        if let Err(e) = stream.seek_range(byte_pos, None).await {
                            tracing::error!("HlsPacketProducer: seek failed: {}", e);
                            // Continue reading, don't fail
                        } else {
                            tracing::info!("HlsPacketProducer: seek completed to byte pos {}", byte_pos);
                            
                            // Clear buffers after seek
                            current_init_bytes.clear();
                            media_buffer.clear();
                            is_collecting_init = false;
                            // Keep current_init_hash to maintain decoder state
                        }
                    }
                    Ok(Some(ProducerCommand::Flush)) => {
                        tracing::debug!("HlsPacketProducer: flushing buffers");
                        current_init_bytes.clear();
                        media_buffer.clear();
                        is_collecting_init = false;
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
                    if !media_buffer.is_empty() {
                        let pkt = Packet {
                            init_hash: current_init_hash,
                            init_bytes: Bytes::new(), // Init already sent
                            media_bytes: Bytes::from(media_buffer),
                            variant_index: None,
                        };
                        let _ = out.send(pkt).await;
                    }
                    return Ok(());
                }
            };

            match msg {
                StreamMsg::Data(bytes) => {
                    if is_collecting_init {
                        // Collecting init segment bytes
                        current_init_bytes.extend_from_slice(&bytes);
                    } else {
                        // Collecting media segment bytes
                        media_buffer.extend_from_slice(&bytes);

                        // Send packet when buffer reaches reasonable size (256KB)
                        if media_buffer.len() >= 256 * 1024 {
                            let pkt = Packet {
                                init_hash: current_init_hash,
                                init_bytes: Bytes::new(), // Init already sent
                                media_bytes: Bytes::from(std::mem::take(&mut media_buffer)),
                                variant_index: None,
                            };
                            if out.send(pkt).await.is_err() {
                                return Ok(()); // Consumer dropped
                            }
                        }
                    }
                }
                StreamMsg::Control(control) => {
                    use stream_download::source::StreamControl;
                    match control {
                        StreamControl::ChunkStart { kind, .. } => {
                            if kind == ChunkKind::Init {
                                tracing::debug!("HlsPacketProducer: init segment started");
                                is_collecting_init = true;
                                current_init_bytes.clear();
                            }
                        }
                        StreamControl::ChunkEnd { kind, .. } => {
                            if kind == ChunkKind::Init && is_collecting_init {
                                is_collecting_init = false;

                                // Compute hash of init segment
                                let mut hasher = DefaultHasher::new();
                                current_init_bytes.hash(&mut hasher);
                                let new_hash = hasher.finish();

                                tracing::debug!(
                                    "HlsPacketProducer: init segment ended, hash={}, size={}",
                                    new_hash,
                                    current_init_bytes.len()
                                );

                                // If hash changed, we need to send init bytes with next packet
                                if new_hash != current_init_hash {
                                    tracing::info!(
                                        "HlsPacketProducer: init hash changed: {} -> {}",
                                        current_init_hash,
                                        new_hash
                                    );
                                    current_init_hash = new_hash;

                                    // Send a packet with init bytes immediately
                                    let pkt = Packet {
                                        init_hash: current_init_hash,
                                        init_bytes: Bytes::from(current_init_bytes.clone()),
                                        media_bytes: Bytes::new(),
                                        variant_index: None,
                                    };
                                    tracing::info!("HlsPacketProducer: sending init packet, hash={}, init_bytes={}", current_init_hash, pkt.init_bytes.len());
                                    if out.send(pkt).await.is_err() {
                                        tracing::error!("HlsPacketProducer: failed to send init packet - consumer dropped");
                                        return Ok(()); // Consumer dropped
                                    }
                                }
                            } else if kind == ChunkKind::Media {
                                // Media segment ended, flush buffer
                                if !media_buffer.is_empty() {
                                    let pkt = Packet {
                                        init_hash: current_init_hash,
                                        init_bytes: Bytes::new(),
                                        media_bytes: Bytes::from(std::mem::take(&mut media_buffer)),
                                        variant_index: None,
                                    };
                                    tracing::debug!("HlsPacketProducer: sending media packet, size={} bytes", pkt.media_bytes.len());
                                    if out.send(pkt).await.is_err() {
                                        tracing::error!("HlsPacketProducer: failed to send media packet - consumer dropped");
                                        return Ok(()); // Consumer dropped
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
