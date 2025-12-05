use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;

use rodio::{OutputStreamBuilder, Sink};
use stream_download_audio::{
    AbrConfig, AudioOptions, AudioStream, HlsConfig, PlayerEvent, VariantMode, adapt_to_rodio,
};
use tracing::metadata::LevelFilter;
use tracing::trace;
use tracing_subscriber::EnvFilter;

fn default_url() -> String {
    env::var("AUDIO_URL").unwrap_or_else(|_| {
        // Default to an HLS stream; pass an MP3 URL to test HTTP playback.
        "https://stream.silvercomet.top/hls/master.m3u8".to_string()
    })
}

fn is_hls(url: &str) -> bool {
    url.to_ascii_lowercase().contains(".m3u8")
}

fn parse_args() -> (String, VariantMode, Option<usize>) {
    // CLI:
    //   rodio_adaptive [URL] [--mode auto|manual] [--variant N]
    let mut args = env::args().skip(1).collect::<Vec<_>>();

    let mut url = default_url();
    let mut mode = "manual".to_string();
    let mut manual_idx: Option<usize> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" if i + 1 < args.len() => {
                mode = args[i + 1].to_lowercase();
                i += 2;
            }
            "--variant" if i + 1 < args.len() => {
                if let Ok(n) = args[i + 1].parse::<usize>() {
                    manual_idx = Some(n);
                }
                i += 2;
            }
            other => {
                if other.starts_with("http://") || other.starts_with("https://") {
                    url = other.to_string();
                }
                i += 1;
            }
        }
    }

    let vm = if mode == "manual" {
        VariantMode::Manual(manual_idx.unwrap_or(0))
    } else {
        VariantMode::Auto
    };

    (url, vm, manual_idx)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::default()
                .add_directive("stream_download_audio=trace".parse()?)
                .add_directive(LevelFilter::INFO.into()),
        )
        .with_line_number(false)
        .with_file(false)
        .init();

    let (url, vm, manual_idx) = parse_args();

    eprintln!("rodio_adaptive (stream-download-audio)");
    eprintln!("  URL: {}", url);
    match vm {
        VariantMode::Auto => eprintln!("  Mode: AUTO"),
        VariantMode::Manual(i) => eprintln!("  Mode: MANUAL (index: {})", i),
    }
    eprintln!(
        "  Detected source: {}",
        if is_hls(&url) { "HLS" } else { "HTTP" }
    );

    // Build audio stream
    let mut opts = AudioOptions::default();
    opts.initial_mode = vm;

    let stream = if is_hls(&url) {
        // HLS backend with default configs.
        AudioStream::from_hls(
            url.clone(),
            opts,
            HlsConfig::default(),
            AbrConfig::default(),
        )
        .await
    } else {
        // HTTP backend (e.g., MP3/AAC/FLAC).
        AudioStream::from_http(url.clone(), opts).await
    };

    // Subscribe to events for visibility
    let events_rx = stream.subscribe_events();
    let shared = Arc::new(Mutex::new(stream));

    // Spawn an event logger
    let rx = events_rx;
    thread::spawn(move || {
        while let Ok(ev) = rx.recv() {
            match ev {
                PlayerEvent::Started => eprintln!("[event] pipeline started"),
                PlayerEvent::VariantSwitched { from, to, reason } => {
                    eprintln!(
                        "[event] variant switched: {:?} -> {} ({})",
                        from, to, reason
                    )
                }
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
                    trace!("[event] buffer: {} frames decoded", decoded_frames);
                }
                PlayerEvent::EndOfStream => {
                    eprintln!("[event] end-of-stream");
                    break;
                }
                PlayerEvent::Error { message } => {
                    eprintln!("[event] ERROR: {}", message);
                }
            }
        }
    });

    // Setup rodio output
    let stream_handle =
        OutputStreamBuilder::open_default_stream().expect("open default audio stream");
    let sink = Sink::connect_new(&stream_handle.mixer());
    sink.play();

    // Adapt AudioStream into a rodio Source and play.
    let source = adapt_to_rodio(shared.clone());
    sink.append(source);

    // Block until finished (EOF). For live streams, this runs indefinitely.
    sink.sleep_until_end();

    Ok(())
}
