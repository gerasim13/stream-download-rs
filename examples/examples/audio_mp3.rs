use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;

use rodio::{OutputStreamBuilder, Sink};
use stream_download_audio::{AudioOptions, AudioStream, PlayerEvent, adapt_to_rodio};
use tracing::trace;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

fn default_url() -> String {
    // Use env var to override if needed. Note that spaces in URLs should be percent-encoded.
    let raw = env::var("AUDIO_MP3_URL").unwrap_or_else(|_| {
        "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3".to_string()
    });

    // If the env var contains spaces, encode them; otherwise return as is.
    if raw.contains(' ') {
        raw.replace(' ', "%20")
    } else {
        raw
    }
}

fn parse_args() -> String {
    // CLI:
    //   audio_mp3 [URL]
    // If URL omitted, default_url() will be used.
    let mut args = env::args().skip(1);
    if let Some(url) = args.next() {
        if url.starts_with("http://") || url.starts_with("https://") {
            if url.contains(' ') {
                url.replace(' ', "%20")
            } else {
                url
            }
        } else {
            default_url()
        }
    } else {
        default_url()
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::default()
                .add_directive("stream_download_audio=trace".parse()?)
                .add_directive(LevelFilter::INFO.into()),
        )
        .with_line_number(true)
        .with_file(true)
        .init();

    let url = parse_args();

    eprintln!("audio_mp3 (stream-download-audio)");
    eprintln!("  URL: {}", url);
    eprintln!("  Source: HTTP (MP3)");

    // Build audio stream for HTTP source (MP3/AAC/FLAC etc.).
    let opts = AudioOptions {
        // Target the device's common sample rate; adjust if needed.
        target_sample_rate: 48_000,
        target_channels: 2,
        ..Default::default()
    };
    let stream = AudioStream::from_http(url.clone(), opts).await;

    // Subscribe to events for visibility.
    let events_rx = stream.subscribe_events();
    let shared = Arc::new(Mutex::new(stream));

    // Setup rodio output.
    let stream_handle =
        OutputStreamBuilder::open_default_stream().expect("open default audio stream");
    let sink = Sink::connect_new(&stream_handle.mixer());
    sink.play();

    // Adapt AudioStream into a rodio Source and play.
    let source = adapt_to_rodio(shared.clone());
    sink.append(source);

    eprintln!("Playing... Press Ctrl+C to quit.");
    // Block until finished (for finite MP3). For live sources, this would run indefinitely.
    sink.sleep_until_end();

    Ok(())
}
