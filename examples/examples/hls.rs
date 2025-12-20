use std::error::Error;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use stream_download::source::DecodeError;
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
                .add_directive("stream_download_hls=trace".parse()?)
                .add_directive(LevelFilter::INFO.into()),
        )
        .with_line_number(false)
        .with_file(false)
        .init();

    let url = "https://stream.silvercomet.top/hls/master.m3u8".to_string();
    let manual_variant_idx = 0;
    let settings = HlsSettings::default().selection_manual(VariantId(manual_variant_idx));
    let params = HlsStreamParams::new(url, settings);

    // Persistent, deterministic on-disk storage layout:
    // `<storage_root>/<master_hash>/<variant_id>/<segment_basename>`
    //
    // The same `storage_root` is also used for persisted small resources written via
    // `StreamControl::StoreResource` (e.g. playlists/keys), using the tree layout:
    // `<storage_root>/<resource_key_as_path>`.
    let storage_root = PathBuf::from("./hls-cache");
    std::fs::create_dir_all(&storage_root)?;

    // Base prefetch buffer size for the adaptive storage wrapper (factory multiplies it by 2).
    let prefetch_bytes = NonZeroUsize::new(8 * 1024 * 1024).unwrap(); // 8MB

    // Limit how many distinct HLS streams we keep in persistent storage (LRU by master playlist).
    let max_cached_streams = NonZeroUsize::new(10).unwrap();

    let reader = match StreamDownload::new::<HlsStream>(
        params,
        HlsPersistentStorageProvider::new_hls_file_tree(
            storage_root,
            prefetch_bytes,
            Some(max_cached_streams),
        ),
        Settings::default(),
    )
    .await
    {
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
