use std::env;
use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::Arc;

use reqwest::Url;
use rodio::{OutputStreamBuilder, Sink};
use stream_download::Settings;
use stream_download::storage::ProvidesStorageHandle;
use stream_download_audio::{AudioSettings, AudioStream, RodioSourceAdapter};
use stream_download_hls::{HlsPersistentStorageProvider, HlsSettings, HlsStream, HlsStreamParams};
use tracing::metadata::LevelFilter;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::default()
                .add_directive("stream_download=info".parse()?)
                .add_directive("stream_download_hls=info".parse()?)
                .add_directive("stream_download_audio=info".parse()?)
                .add_directive(LevelFilter::INFO.into()),
        )
        .with_line_number(false)
        .with_file(false)
        .init();

    let url: Url = "https://stream.silvercomet.top/hls/master.m3u8".parse()?;

    // HLS needs a StorageHandle for playlists/keys caching.
    //
    // `TempStorageProvider` does not provide a `StorageHandle`, so for HLS we use a segmented
    // storage provider that can vend a tree-layout handle.
    let storage_root = env::temp_dir().join("stream-download-audio-hls-example");
    let storage = HlsPersistentStorageProvider::new_hls_file_tree(
        storage_root,
        NonZeroUsize::new(256 * 1024).expect("non-zero prefetch bytes"),
        None,
    );

    let storage_handle = storage.storage_handle().expect("storage handle");

    // HLS settings (ABR enabled by default).
    let hls_settings = Arc::new(HlsSettings::default());
    let params = HlsStreamParams::new(url, hls_settings, storage_handle);

    // stream-download settings for the underlying HLS stream.
    let stream_settings: Settings<HlsStream> = Settings::default();

    // Audio buffering/settings.
    let audio_settings = AudioSettings {
        queue_capacity_chunks: 8,
        target_channels: 2,
        target_sample_rate: 48_000,
    };

    println!("Creating AudioStream (HLS)...");
    let stream = AudioStream::new_hls(params, storage, stream_settings, audio_settings).await?;
    println!("AudioStream created.");

    println!("Setting up Rodio output...");
    let stream_handle =
        OutputStreamBuilder::open_default_stream().expect("open default audio stream");
    let sink = Sink::connect_new(&stream_handle.mixer());
    sink.set_volume(0.05);

    println!("Creating RodioSourceAdapter...");
    let source = RodioSourceAdapter::new(stream);
    sink.append(source);
    sink.play();
    sink.sleep_until_end();

    Ok(())
}
