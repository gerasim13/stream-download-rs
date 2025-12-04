use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use stream_download_hls::{
    AdaptiveHlsAudio, AdaptiveHlsAudioOptions, FloatSampleSource, PlayerEvent, VariantMode,
};

use rodio::{OutputStreamBuilder, Sink, Source};

fn default_url() -> String {
    env::var("HLS_URL").unwrap_or_else(|_| {
        // Public live MP3 HLS radio stream; override with HLS_URL if needed.
        "https://streams.radiomast.io/ref-128k-mp3-stereo/hls.m3u8".to_string()
    })
}

struct RodioSourceAdapter {
    inner: Arc<Mutex<AdaptiveHlsAudio>>,
    buf: Vec<f32>,
    pos: usize,
    chunk_samples: usize,
    sample_rate: u32,
    channels: u16,
    // If EOF is reached, stop iteration.
    eof: bool,
}

impl RodioSourceAdapter {
    fn new(inner: Arc<Mutex<AdaptiveHlsAudio>>, chunk_samples: usize) -> Self {
        // Lock once to read initial spec (may change later; rodio assumes static).
        let spec = inner.lock().unwrap().spec();
        Self {
            inner,
            buf: Vec::with_capacity(chunk_samples),
            pos: 0,
            chunk_samples,
            sample_rate: spec.sample_rate,
            channels: spec.channels,
            eof: false,
        }
    }
}

impl Iterator for RodioSourceAdapter {
    type Item = f32;

    fn next(&mut self) -> Option<f32> {
        if self.eof {
            return None;
        }

        if self.pos >= self.buf.len() {
            // Refill buffer from the source.
            self.buf.resize(self.chunk_samples, 0.0);
            let read = {
                let mut guard = self.inner.lock().unwrap();
                guard.read_interleaved(&mut self.buf)
            };

            if read == 0 {
                // The source may be at EOF (VOD finished) or not yet produced any data.
                // Sleep briefly and try again on next call to avoid busy looping.
                thread::sleep(Duration::from_millis(10));
                // If it keeps returning 0 after some time, rodio would keep polling.
                // Mark EOF here to signal completion for VOD/end-of-stream.
                self.eof = true;
                return None;
            }

            self.buf.truncate(read);
            self.pos = 0;
        }

        let s = self.buf[self.pos];
        self.pos += 1;
        Some(s)
    }
}

impl Source for RodioSourceAdapter {
    fn current_span_len(&self) -> Option<usize> {
        None
    }

    fn channels(&self) -> u16 {
        self.channels
    }

    fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    fn total_duration(&self) -> Option<std::time::Duration> {
        None
    }
}

fn parse_args() -> (String, VariantMode, Option<usize>) {
    // CLI:
    //   rodio_adaptive [URL] [--mode auto|manual] [--variant N]
    let mut args = env::args().skip(1).collect::<Vec<_>>();

    let mut url = default_url();
    let mut mode = "auto".to_string();
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let (url, vm, manual_idx) = parse_args();

    eprintln!("rodio_adaptive");
    eprintln!("  URL: {}", url);
    match vm {
        VariantMode::Auto => eprintln!("  Mode: AUTO"),
        VariantMode::Manual(i) => eprintln!("  Mode: MANUAL (index: {})", i),
    }

    // Build adaptive audio source
    let mut opts = AdaptiveHlsAudioOptions::default();
    opts.initial_mode = vm;
    // Keep modest buffer targets for responsive startup
    opts.target_decoded_ms = 400;
    opts.target_compressed_ms = 1200;

    let mut src = AdaptiveHlsAudio::new(url, opts)?;
    src.start()?;

    // Subscribe to events for visibility
    let events_rx = src.subscribe_events();
    let shared = Arc::new(Mutex::new(src));

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
                    eprintln!("[event] buffer: {} frames decoded", decoded_frames);
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
        rodio::OutputStreamBuilder::open_default_stream().expect("open default audio stream");
    let sink = rodio::Sink::connect_new(&stream_handle.mixer());

    // Adapter pulls from FloatSampleSource and feeds rodio
    // A chunk of ~4096 samples is a good balance
    let adapter = RodioSourceAdapter::new(shared.clone(), 4096);
    sink.append(adapter);

    // Block until finished (EOF). For live streams, this runs indefinitely.
    sink.sleep_until_end();

    Ok(())
}
