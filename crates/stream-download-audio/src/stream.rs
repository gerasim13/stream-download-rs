/*!
AudioStream implementation: unified float PCM source backed by a ring buffer.

This module provides the high-level `AudioStream` type that:
- exposes a pull-based API (`FloatSampleSource`) returning interleaved f32 PCM,
- can be constructed from either an HLS URL (via `stream-download-hls`) or a single HTTP URL (via `stream-download`),
- maintains a lock-free SPSC ring buffer for low-latency audio delivery,
- emits player events via `kanal`,
- anticipates a future processing chain and background pipeline (decode, resample, effects).

Status:
- This is a skeleton implementation that wires up the public API and buffering.
- The actual background pipeline (I/O, decode, resample, effects) will be integrated later.
*/

use std::sync::{Arc, Mutex};
use std::thread;

use kanal as kchan;
use ringbuf::traits::{Consumer, Producer, Split};
use ringbuf::{HeapCons, HeapProd, HeapRb};
use tracing::{debug, error, info, trace};

use crate::api::{AudioOptions, AudioProcessor, AudioSpec, FloatSampleSource, PlayerEvent};
use crate::backends::http::{extension_from_url, open_http_seek_gated_mss};
use stream_download_hls::{AbrConfig, HlsConfig};

use symphonia::core::audio::GenericAudioBufferRef;
use symphonia::core::codecs::audio::AudioDecoderOptions;
use symphonia::core::formats::{FormatOptions, TrackType};
use symphonia::core::meta::MetadataOptions;
use symphonia::default::{get_codecs, get_probe};
use symphonia_core::codecs::CodecParameters;
use symphonia_core::formats::probe::Hint;

/// Internal backend selector for the audio source.
#[derive(Debug, Clone)]
pub(crate) enum Backend {
    Hls {
        url: String,
        hls_config: HlsConfig,
        abr_config: AbrConfig,
    },
    Http {
        url: String,
    },
}

/// Simple broadcast hub for `PlayerEvent` using `kanal`.
#[derive(Debug)]
pub(crate) struct EventHub {
    subs: Mutex<Vec<kchan::Sender<PlayerEvent>>>,
}

impl EventHub {
    pub fn new() -> Self {
        Self {
            subs: Mutex::new(Vec::new()),
        }
    }

    pub fn subscribe(&self) -> kchan::Receiver<PlayerEvent> {
        let (tx, rx) = kchan::unbounded::<PlayerEvent>();
        self.subs.lock().unwrap().push(tx);
        rx
    }

    pub fn send(&self, ev: PlayerEvent) {
        let subs = self.subs.lock().unwrap();
        for tx in subs.iter() {
            let _ = tx.send(ev.clone());
        }
    }
}

/// High-level audio stream backed by a lock-free ring buffer.
///
/// This type implements `FloatSampleSource` and is intended to be fed by a background
/// pipeline (I/O, decode, resample, effects). In this skeleton, only buffering and API
/// surface are implemented.
pub struct AudioStream {
    spec: Arc<Mutex<AudioSpec>>,
    producer: Arc<Mutex<HeapProd<f32>>>,
    consumer: HeapCons<f32>,
    events: Arc<EventHub>,

    pub(crate) backend: Backend,

    // Processing chain (skeleton; not applied yet)
    pub(crate) processors: Arc<Mutex<Vec<Arc<dyn AudioProcessor>>>>,
}

impl std::fmt::Debug for AudioStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AudioStream")
            .field("spec", &self.spec.lock().unwrap())
            .field("backend", &self.backend)
            .finish()
    }
}

impl AudioStream {
    /// Create an AudioStream from an HLS master playlist URL.
    ///
    /// This skeleton only initializes state; background tasks are TODO.
    pub fn from_hls(
        url: impl Into<String>,
        opts: AudioOptions,
        hls_config: HlsConfig,
        abr_config: AbrConfig,
    ) -> Self {
        let url = url.into();

        let spec = Arc::new(Mutex::new(AudioSpec {
            sample_rate: opts.target_sample_rate,
            channels: opts.target_channels,
        }));

        let cap = opts
            .ring_capacity_frames
            .saturating_mul(opts.target_channels as usize);
        let rb = HeapRb::<f32>::new(cap.max(1)); // avoid zero-capacity
        let (prod, cons) = rb.split();

        let events = Arc::new(EventHub::new());

        Self {
            spec,
            producer: Arc::new(Mutex::new(prod)),
            consumer: cons,
            events,
            backend: Backend::Hls {
                url,
                hls_config,
                abr_config,
            },
            processors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Create an AudioStream from a regular HTTP URL (e.g., MP3/AAC/FLAC).
    ///
    /// This skeleton only initializes state; background tasks are TODO.
    pub fn from_http(url: impl Into<String>, opts: AudioOptions) -> Self {
        let url = url.into();

        let spec = Arc::new(Mutex::new(AudioSpec {
            sample_rate: opts.target_sample_rate,
            channels: opts.target_channels,
        }));

        let cap = opts
            .ring_capacity_frames
            .saturating_mul(opts.target_channels as usize);
        let rb = HeapRb::<f32>::new(cap.max(1));
        let (prod, cons) = rb.split();

        let events = Arc::new(EventHub::new());
        let producer = Arc::new(Mutex::new(prod));

        // Clone state for background decoding worker.
        let url_clone = url.clone();
        let events_clone = events.clone();
        let spec_clone = spec.clone();
        let producer_clone = producer.clone();

        // Spawn decoding worker on a dedicated thread with a Tokio runtime.
        thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    error!("HTTP backend: tokio runtime build failed: {e}");
                    events_clone.send(PlayerEvent::Error {
                        message: format!("tokio runtime error: {e}"),
                    });
                    return;
                }
            };

            rt.block_on(async move {
                // Open a seek-gated MediaSourceStream.
                debug!("HTTP backend: opening seek-gated MSS for URL: {}", url_clone);
                let (mss, gate) = match open_http_seek_gated_mss(&url_clone).await {
                    Ok(v) => v,
                    Err(e) => {
                        error!("HTTP backend: open failed: {e}");
                        events_clone.send(PlayerEvent::Error {
                            message: format!("open failed: {e}"),
                        });
                        return;
                    }
                };
                debug!("HTTP backend: MSS created, starting probe...");

                // Prepare a probe hint using the URL extension (if any).
                let mut hint = Hint::new();
                if let Some(ext) = extension_from_url(&url_clone) {
                    hint.with_extension(&ext);
                }

                // Probe and open the container.
                let mut format = match get_probe().probe(
                    &hint,
                    mss,
                    FormatOptions::default(),
                    MetadataOptions::default(),
                ) {
                    Ok(f) => f,
                    Err(e) => {
                        error!("HTTP backend: probe failed: {e}");
                        events_clone.send(PlayerEvent::Error {
                            message: format!("probe failed: {e}"),
                        });
                        return;
                    }
                };
                debug!(
                    "HTTP backend: probe OK, format='{}'",
                    format.format_info().short_name
                );

                let container_name = Some(format.format_info().short_name.to_string());
                debug!("HTTP backend: selecting default audio track...");

                // Select the default audio track.
                let (track_id, owned_audio_params) = match format.default_track(TrackType::Audio) {
                    Some(t) => match t.codec_params.as_ref().and_then(|cp| cp.audio()).cloned() {
                        Some(ap) => {
                            debug!("HTTP backend: selected track id={} sr={:?} ch={:?}", t.id, ap.sample_rate, ap.channels.clone().map(|c| c.count()));
                            (t.id, ap)
                        }
                        None => {
                            error!("HTTP backend: selected track is not audio");
                            events_clone.send(PlayerEvent::Error {
                                message: "no audio params on selected track".into(),
                            });
                            return;
                        }
                    },
                    None => {
                        error!("HTTP backend: no default audio track");
                        events_clone.send(PlayerEvent::Error {
                            message: "no default audio track".into(),
                        });
                        return;
                    }
                };

                // Build the decoder using dev-0.6 API with owned audio params.
                let dec_opts = AudioDecoderOptions::default();
                debug!("HTTP backend: creating audio decoder...");
                let mut decoder =
                    match get_codecs().make_audio_decoder(&owned_audio_params, &dec_opts) {
                        Ok(d) => d,
                        Err(e) => {
                            error!("HTTP backend: decoder make failed: {e}");
                            events_clone.send(PlayerEvent::Error {
                                message: format!("decoder make failed: {e}"),
                            });
                            return;
                        }
                    };
                debug!("HTTP backend: decoder created, enabling seek gate.");

                // Enable seeking now that the decoder is initialized.
                gate.enable_seek();
                debug!("HTTP backend: seek gate enabled, entering decode loop.");

                // Defer FormatChanged until after the first decoded frame to get accurate spec.

                // Temporary interleaved buffer for converted f32 samples.
                let mut tmp: Vec<f32> = Vec::new();
                let mut signalled_format = false;

                // Decode loop: read packets, decode, convert to f32, push into ring buffer.
                loop {
                    match format.next_packet() {
                        Ok(Some(packet)) => {
                            if packet.track_id() != track_id {
                                continue;
                            }

                            match decoder.decode(&packet) {
                                Ok(decoded) => {
                                    // Convert to a generic buffer reference and then to interleaved f32.
                                    let gab: GenericAudioBufferRef = decoded;
                                    let chans = gab.num_planes();
                                    let frames = gab.frames();
                                    let needed = chans * frames;
                                    trace!(
                                        "HTTP backend: decoded {} frames ({} ch)",
                                        frames, chans
                                    );

                                    // Resize temp buffer to exact length and copy as interleaved f32.
                                    tmp.resize(needed, 0.0);
                                    gab.copy_to_slice_interleaved::<f32, _>(&mut tmp[..needed]);

                                    // Emit format on first decoded frame.
                                    if !signalled_format {
                                        let ds = gab.spec();
                                        let new_spec = AudioSpec {
                                            sample_rate: ds.rate(),
                                            channels: ds.channels().count() as u16,
                                        };
                                        *spec_clone.lock().unwrap() = new_spec;
                                        debug!(
                                            "HTTP backend: first audio frame -> FormatChanged rate={}Hz ch={}",
                                            new_spec.sample_rate, new_spec.channels
                                        );
                                        events_clone.send(PlayerEvent::FormatChanged {
                                            sample_rate: new_spec.sample_rate,
                                            channels: new_spec.channels,
                                            codec: None,
                                            container: container_name.clone(),
                                        });
                                        events_clone.send(PlayerEvent::Started);
                                        signalled_format = true;
                                    }

                                    // Push into the ring buffer without blocking.
                                    let mut pushed = 0usize;
                                    if needed > 0 {
                                        if let Ok(mut prod) = producer_clone.lock() {
                                            let mut i = 0usize;
                                            while i < needed {
                                                if prod.try_push(tmp[i]).is_ok() {
                                                    pushed += 1;
                                                    i += 1;
                                                } else {
                                                    // Ring full: wait for consumer to drain a bit instead of dropping samples.
                                                    std::thread::sleep(std::time::Duration::from_millis(5));
                                                }
                                            }
                                        }
                                    }

                                    if pushed > 0 {
                                        trace!("HTTP backend: pushed {} samples to ring (~{} frames)", pushed, pushed / chans);
                                        events_clone.send(PlayerEvent::BufferLevel {
                                            decoded_frames: pushed / chans,
                                        });
                                    }
                                }
                                Err(e) => {
                                    // Non-fatal decode error; continue.
                                    debug!("HTTP backend: decode error: {e}");
                                }
                            }
                        }
                        Ok(None) => {
                            // End of stream reached.
                            info!("HTTP backend: end of stream");
                            events_clone.send(PlayerEvent::EndOfStream);
                            break;
                        }
                        Err(e) => {
                            error!("HTTP backend: next_packet error: {e}");
                            events_clone.send(PlayerEvent::Error {
                                message: format!("next_packet error: {e}"),
                            });
                            break;
                        }
                    }
                }
            });
        });

        Self {
            spec,
            producer,
            consumer: cons,
            events,
            backend: Backend::Http { url },
            processors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Subscribe to player events.
    pub fn subscribe_events(&self) -> kchan::Receiver<PlayerEvent> {
        self.events.subscribe()
    }

    /// Convenience getter for the current output spec.
    pub fn output_spec(&self) -> AudioSpec {
        *self.spec.lock().unwrap()
    }

    /// Push helper for tests or manual feeding (not part of final API necessarily).
    pub fn push_samples_for_test(&mut self, samples: &[f32]) -> usize {
        let mut written = 0usize;
        for &s in samples {
            if self.producer.lock().unwrap().try_push(s).is_err() {
                break;
            }
            written += 1;
        }
        if written > 0 {
            self.events.send(PlayerEvent::BufferLevel {
                decoded_frames: written / self.spec.lock().unwrap().channels as usize,
            });
        }
        written
    }

    /// Register a processor (no-op for now; not applied in the skeleton).
    pub fn add_processor(&mut self, p: Arc<dyn AudioProcessor>) {
        self.processors.lock().unwrap().push(p);
    }

    /// Internal: pop a single sample (for adapters that need one-by-one consumption).
    pub(crate) fn pop_one(&mut self) -> Option<f32> {
        self.consumer.try_pop()
    }

    /// Internal: pop up to `out.len()` samples into `out`, returning the count.
    pub(crate) fn pop_chunk(&mut self, out: &mut [f32]) -> usize {
        let mut n = 0usize;
        while n < out.len() {
            match self.consumer.try_pop() {
                Some(s) => {
                    out[n] = s;
                    n += 1;
                }
                None => break,
            }
        }
        n
    }
}

impl FloatSampleSource for AudioStream {
    fn read_interleaved(&mut self, out: &mut [f32]) -> usize {
        let n = self.pop_chunk(out);
        if n < out.len() {
            trace!("AudioStream underrun: requested {}, got {}", out.len(), n);
        }
        n
    }

    fn spec(&self) -> AudioSpec {
        *self.spec.lock().unwrap()
    }
}
