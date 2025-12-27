use std::env;
use std::error::Error;

use rodio::{OutputStreamBuilder, Sink};
use stream_download::storage::ProvidesStorageHandle;
use stream_download::storage::temp::TempStorageProvider;
use stream_download::{Settings, StreamDownload};
use stream_download_audio::{
    AudioDecodeOptions, AudioDecodeStream, RodioSourceAdapter, TapStorageProvider,
};
use stream_download_hls::{HlsSettings, HlsStream, HlsStreamParams};
use tokio::sync::mpsc;
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

    // Example HLS master playlist.
    let url = "https://stream.silvercomet.top/hls/master.m3u8".parse()?;

    // Storage root for persistent HLS caching (playlists, keys, segments).
    let storage_root = env::temp_dir().join("stream-download-audio-hls-example");

    // HLS settings (ABR enabled by default).
    let hls_settings = HlsSettings::default();

    // Decoder/buffering options.
    // NOTE: these values are meaningful units (bytes + samples), not arbitrary fixed sizes.
    let opts = AudioDecodeOptions::default();

    // In-band control tap: required to surface ordered HLS init/media boundaries from stream-download
    // into the audio layer (without relying on out-of-band events).
    let (ctrl_tx, ctrl_rx) = mpsc::channel(256);

    // Build StreamDownload over stream-download-hls::HlsStream so you can fully control storage/settings.
    //
    // Storage provider:
    // - we use TempStorageProvider for this example, but you can use any provider you want.
    // - TapStorageProvider forwards in-band StreamControl messages to `ctrl_rx`.
    let storage = TapStorageProvider::new(TempStorageProvider::new(), ctrl_tx);

    println!("Creating StreamDownload (HLS)...");
    let params = HlsStreamParams::new(
        url,
        hls_settings,
        storage.storage_handle().expect("storage handle"),
    );
    let reader = StreamDownload::new::<HlsStream>(params, storage, Settings::default()).await?;

    println!("Creating AudioDecodeStream (HLS)...");
    let stream = AudioDecodeStream::new_from_stream_download(reader, ctrl_rx, opts).await?;
    println!("AudioDecodeStream created.");

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
