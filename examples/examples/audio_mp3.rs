use std::error::Error;

use reqwest::Url;
use rodio::{OutputStreamBuilder, Sink};
use stream_download::Settings;
use stream_download::http::HttpStream;
use stream_download::storage::temp::TempStorageProvider;
use stream_download_audio::{AudioSettings, AudioStream, RodioSourceAdapter};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::default()
                .add_directive("stream_download=info".parse()?)
                .add_directive("stream_download_audio=info".parse()?)
                .add_directive(LevelFilter::INFO.into()),
        )
        .with_line_number(true)
        .with_file(true)
        .init();

    // Progressive HTTP audio (MP3/AAC/FLAC etc.)
    let url: Url = "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3".parse()?;

    // stream-download settings for the underlying HTTP stream.
    let stream_settings: Settings<HttpStream<stream_download::http::reqwest::Client>> =
        Settings::default();

    // Audio buffering/settings.
    let audio_settings = AudioSettings {
        queue_capacity_chunks: 8,
        target_channels: 2,
        target_sample_rate: 48_000,
    };

    // Storage for stream-download buffering.
    let storage = TempStorageProvider::default();

    println!("Creating AudioStream (HTTP)...");
    let stream = AudioStream::new_http(url, storage, stream_settings, audio_settings).await?;
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
