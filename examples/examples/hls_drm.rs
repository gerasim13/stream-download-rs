use std::env;
use std::error::Error;
use std::io::{self, ErrorKind};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use reqwest::Url;
use rodio::cpal::FromSample;
use stream_download::source::DecodeError;
use stream_download::storage::ProvidesStorageHandle;
use stream_download::{Settings, StreamDownload};
use stream_download_hls::{
    HlsPersistentStorageProvider, HlsSettings, HlsStream, HlsStreamParams, VariantId,
};
use tracing::metadata::LevelFilter;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::default()
                .add_directive("stream_download_hls=debug".parse()?)
                .add_directive(LevelFilter::INFO.into()),
        )
        .with_line_number(false)
        .with_file(false)
        .init();

    let url = "https://stream.silvercomet.top/drm/master.m3u8";
    let url = Url::parse(url)
        .map_err(|e| io::Error::new(ErrorKind::Other, format!("invalid HLS url '{}': {e}", url)))?;
    let manual_variant_idx = 0;
    let settings = Settings::default();
    let hls_settings = HlsSettings::default()
        .variant_stream_selector(move |_| Some(VariantId(manual_variant_idx)));
    // .key_processor_cb(Some(Arc::new(Box::new(|key| {
    //     // this will panic for sure
    //     Bytes::new()
    // }))));

    // Persistent, deterministic on-disk storage layout:
    // `<storage_root>/<master_hash>/<variant_id>/<segment_basename>`
    //
    // The same `storage_root` is also used for persisted small resources written via
    // `StreamControl::StoreResource` (e.g. playlists/keys), using the tree layout:
    // `<storage_root>/<resource_key_as_path>`.
    let storage_root = env::temp_dir().join("hls-cache");
    let prefetch_bytes = NonZeroUsize::new((settings.get_prefetch_bytes() * 2) as usize).unwrap();
    let max_cached_streams = NonZeroUsize::new(10).unwrap();

    // Build the persistent storage provider first, then extract a StorageHandle for
    // read-before-fetch caching of playlists/keys inside HLS.
    let provider = HlsPersistentStorageProvider::new_hls_file_tree(
        storage_root.clone(),
        prefetch_bytes,
        Some(max_cached_streams),
    );
    let storage_handle = provider
        .storage_handle()
        .expect("HLS persistent storage provider must vend a StorageHandle");

    let params = HlsStreamParams::new(url, hls_settings, storage_handle);
    let reader = match StreamDownload::new::<HlsStream>(params, provider, settings).await {
        Ok(r) => r,
        Err(e) => {
            return Err(e.decode_error().await)?;
        }
    };

    let handle = tokio::task::spawn_blocking(move || {
        let stream_handle = rodio::OutputStreamBuilder::open_default_stream()?;
        let sink = rodio::Sink::connect_new(stream_handle.mixer());
        sink.append(rodio::Decoder::new(reader)?);
        sink.play();
        sink.sleep_until_end();

        Ok::<_, Box<dyn Error + Send + Sync>>(())
    });
    handle.await??;

    Ok(())
}
