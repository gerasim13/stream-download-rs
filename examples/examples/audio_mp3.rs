use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;

use rodio::{OutputStreamBuilder, Sink};
use stream_download_audio::{AudioOptions, AudioStream, RodioSourceAdapter};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::default()
                .add_directive("stream_download_audio=info".parse()?)
                .add_directive(LevelFilter::INFO.into()),
        )
        .with_line_number(true)
        .with_file(true)
        .init();

    // Build audio stream for HTTP source (MP3/AAC/FLAC etc.).
    let url = "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3".to_string();
    let opts = AudioOptions::default();
    let stream = AudioStream::from_http(url.clone(), opts).await;

    // Setup rodio output.
    let stream_handle =
        OutputStreamBuilder::open_default_stream().expect("open default audio stream");
    let sink = Sink::connect_new(&stream_handle.mixer());

    // Adapt AudioStream into a rodio Source and play.
    let shared = Arc::new(Mutex::new(stream));
    let source = RodioSourceAdapter::new(shared.clone());
    sink.append(source);
    sink.play();
    sink.sleep_until_end();

    Ok(())
}
