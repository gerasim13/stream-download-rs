use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;

use rodio::{OutputStreamBuilder, Sink};

use stream_download_audio::{AudioOptions, AudioStream, PlayerEvent, adapt_to_rodio};

fn default_url() -> String {
    // Use env var to override if needed. Note that spaces in URLs should be percent-encoded.
    let raw = env::var("AUDIO_MP3_URL").unwrap_or_else(|_| {
        // Original test URL (with spaces) from the prompt:
        // "http://www.hyperion-records.co.uk/audiotest/14 Clementi Piano Sonata in D major, Op 25 No 6 - Movement 2 Un poco andante.MP3"
        // We'll return a percent-encoded variant to ensure proper fetching.
        "http://www.hyperion-records.co.uk/audiotest/14%20Clementi%20Piano%20Sonata%20in%20D%20major,%20Op%2025%20No%206%20-%20Movement%202%20Un%20poco%20andante.MP3".to_string()
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

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
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
    let stream = AudioStream::from_http(url.clone(), opts);

    // Subscribe to events for visibility.
    let events_rx = stream.subscribe_events();
    let shared = Arc::new(Mutex::new(stream));

    // Spawn an event logger.
    thread::spawn(move || {
        while let Ok(ev) = events_rx.recv() {
            match ev {
                PlayerEvent::Started => eprintln!("[event] pipeline started"),
                PlayerEvent::FormatChanged {
                    sample_rate,
                    channels,
                    codec,
                    container,
                } => {
                    eprintln!(
                        "[event] format changed: {} Hz, {} ch, codec={:?}, container={:?}",
                        sample_rate, channels, codec, container
                    );
                }
                PlayerEvent::BufferLevel { decoded_frames } => {
                    eprintln!("[event] buffer: {} frames decoded", decoded_frames);
                }
                PlayerEvent::EndOfStream => {
                    eprintln!("[event] end-of-stream");
                    break;
                }
                PlayerEvent::VariantSwitched { .. } => {
                    // Not relevant for HTTP, but included for completeness.
                }
                PlayerEvent::Error { message } => {
                    eprintln!("[event] ERROR: {}", message);
                }
            }
        }
    });

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
