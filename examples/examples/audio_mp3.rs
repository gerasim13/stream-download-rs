use std::error::Error;
use std::time::Duration;

use rodio::{OutputStreamBuilder, Sink};
use stream_download_audio::{AudioDecodeOptions, AudioDecodeStream, RodioSourceAdapter};
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

    // Progressive HTTP audio (MP3/AAC/FLAC etc.)
    let url = "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3".parse()?;

    // Decoder/buffering options.
    // NOTE: these are meaningful units (bytes + samples), not arbitrary fixed sizes.
    let opts = AudioDecodeOptions::default();

    println!("Creating AudioDecodeStream (HTTP)...");
    let stream = AudioDecodeStream::new_http(url, opts).await?;
    println!("AudioDecodeStream created.");

    // Setup rodio output
    let stream_handle =
        OutputStreamBuilder::open_default_stream().expect("open default audio stream");
    let sink = Sink::connect_new(&stream_handle.mixer());

    // Adapt AudioDecodeStream into a rodio Source and play.
    let source = RodioSourceAdapter::new(stream);
    sink.append(source);
    sink.play();
    sink.sleep_until_end();

    Ok(())
}
