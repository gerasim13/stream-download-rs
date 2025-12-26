use std::env;
use std::error::Error;
use std::time::Duration;

use rodio::{OutputStreamBuilder, Sink};
use stream_download_audio::{AudioDecodeOptions, AudioDecodeStream, RodioSourceAdapter};
use stream_download_hls::HlsSettings;
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

    println!("Creating AudioDecodeStream (HLS)...");
    let stream = AudioDecodeStream::new_hls(url, hls_settings, opts, Some(storage_root)).await?;
    println!("AudioDecodeStream created.");

    println!("Setting up Rodio output...");
    let stream_handle =
        OutputStreamBuilder::open_default_stream().expect("open default audio stream");
    let sink = Sink::connect_new(&stream_handle.mixer());
    sink.set_volume(0.01);

    println!("Creating RodioSourceAdapter...");
    let source = RodioSourceAdapter::new(stream);
    sink.append(source);
    sink.play();

    // Keep the process alive.
    // For production apps you should implement a proper shutdown signal / UI loop.
    tokio::time::sleep(Duration::from_secs(60)).await;

    Ok(())
}
