use async_trait::async_trait;
use bytes::Bytes;
use kanal::AsyncSender;
use tokio_util::sync::CancellationToken;
use url::Url;

use stream_download::source::DecodeError;
use stream_download::storage::ProvidesStorageHandle;
use stream_download::{Settings, StreamDownload};
use stream_download_hls::{HlsPersistentStorageProvider, HlsSettings, HlsStream, HlsStreamParams};

use crate::backends::common::{io_other, run_blocking_reading_loop};
use crate::pipeline::Packet;

/// HLS packet producer implementation.
pub struct HlsPacketProducer {
    url: String,
    settings: HlsSettings,
}

impl HlsPacketProducer {
    /// Create a new HLS packet producer.
    pub fn new(url: impl Into<String>, settings: HlsSettings) -> Self {
        Self {
            url: url.into(),
            settings,
        }
    }
}

#[async_trait]
impl crate::backends::PacketProducer for HlsPacketProducer {
    /// Async HLS packet producer for the unified pipeline.
    /// Emits self-contained Packet { init_hash, init_bytes, media_bytes } into `out`.
    ///
    /// For HLS, we use StreamDownload::new::<HlsStream> to create a stream that
    /// handles HLS-specific logic (playlist parsing, segment downloading, ABR).
    /// The HlsStream produces raw bytes from media segments which we then
    /// package into Packets.
    async fn run(
        &mut self,
        out: AsyncSender<Packet>,
        cancel: Option<CancellationToken>,
    ) -> std::io::Result<()> {
        // Persistent storage provider + StorageHandle are now mandatory for HLS so that
        // read-before-fetch caching of playlists/keys is enabled.
        //
        // Note: this backend currently uses a fixed on-disk root. If you want to configure it,
        // we can thread it through `HlsPacketProducer::new` / settings.
        let storage_root = std::path::PathBuf::from("./hls-audio-cache");
        std::fs::create_dir_all(&storage_root)?;

        let prefetch_bytes = std::num::NonZeroUsize::new(8 * 1024 * 1024).unwrap();
        let max_cached_streams = std::num::NonZeroUsize::new(10).unwrap();

        let provider = HlsPersistentStorageProvider::new_hls_file_tree(
            storage_root,
            prefetch_bytes,
            Some(max_cached_streams),
        );

        let storage_handle = provider
            .storage_handle()
            .expect("HLS persistent storage provider must vend a StorageHandle");

        let url = Url::parse(&self.url)
            .map_err(|e| io_other(&format!("invalid HLS url '{}': {e}", self.url)))?;
        let params = HlsStreamParams::new(url, self.settings.clone(), storage_handle);

        let reader =
            match StreamDownload::new::<HlsStream>(params, provider, Settings::default()).await {
                Ok(r) => r,
                Err(e) => {
                    let msg: String = e.decode_error().await;
                    return Err(io_other(&msg));
                }
            };

        // Run the blocking reading loop in a separate thread
        // This uses the same pattern as HttpPacketProducer
        let sync_out = out.clone_sync();
        tokio::task::spawn_blocking(move || {
            run_blocking_reading_loop(
                reader,
                sync_out,
                cancel,
                None,         // variant_index - simplified for now
                0,            // init_hash - simplified for now
                Bytes::new(), // init_bytes - simplified for now
            )
        })
        .await
        .map_err(|join_err| io_other(&format!("join error: {join_err}")))?
    }
}
