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

use kanal as kchan;
use ringbuf::traits::{Consumer, Producer, Split};
use ringbuf::{HeapCons, HeapProd, HeapRb};
use tracing::{debug, error, info, trace};

use crate::api::{AudioOptions, AudioProcessor, AudioSpec, FloatSampleSource, PlayerEvent};
use crate::backends::http::{extension_from_url, open_http_seek_gated_mss};
use stream_download_hls::{AbrConfig, HlsConfig};

use symphonia::core::audio::GenericAudioBufferRef;
use symphonia::core::codecs::audio::AudioDecoderOptions;
use symphonia::core::formats::probe::Hint;
use symphonia::core::formats::{FormatOptions, TrackType};
use symphonia::core::meta::MetadataOptions;
use symphonia::default::{get_codecs, get_probe};

/// Small trait to unify enabling seek after decoder init for different controllers.
trait SeekEnable: Send + Sync + 'static {
    fn enable_seek_gate(&self);
}

/// Blanket impl for HTTP seek gate handle.
impl SeekEnable for crate::backends::http::SeekGateHandle {
    fn enable_seek_gate(&self) {
        self.enable_seek();
    }
}

/// Blanket impl for HLS source controller.
impl SeekEnable for crate::backends::hls::HlsSourceController {
    fn enable_seek_gate(&self) {
        self.enable_seek();
    }
}

/// Spawn a blocking decode worker that probes, decodes, converts to f32 and pushes into the ring.
/// This unifies HTTP/HLS decode loop in one place.
fn spawn_decode_worker<C>(
    mut format: Box<dyn symphonia::core::formats::FormatReader + Send + Sync>,
    controller: C,
    producer: Arc<Mutex<HeapProd<f32>>>,
    events: Arc<EventHub>,
    spec_out: Arc<Mutex<AudioSpec>>,
    container_name: Option<String>,
) where
    C: SeekEnable,
{
    tokio::task::spawn_blocking(move || {
        // Select the default audio track.
        let (mut track_id, owned_audio_params) = match format.default_track(TrackType::Audio) {
            Some(t) => match t.codec_params.as_ref().and_then(|cp| cp.audio()).cloned() {
                Some(ap) => {
                    trace!(
                        "decode: selected track id={} sr={:?} ch={:?}",
                        t.id,
                        ap.sample_rate,
                        ap.channels.clone().map(|c| c.count())
                    );
                    (t.id, ap)
                }
                None => {
                    error!("decode: selected track is not audio");
                    events.send(PlayerEvent::Error {
                        message: "no audio params on selected track".into(),
                    });
                    return;
                }
            },
            None => {
                error!("decode: no default audio track");
                events.send(PlayerEvent::Error {
                    message: "no default audio track".into(),
                });
                return;
            }
        };

        // Build the decoder using dev-0.6 API with owned audio params.
        let dec_opts = AudioDecoderOptions::default();
        trace!("decode: creating audio decoder...");
        let mut decoder = match get_codecs().make_audio_decoder(&owned_audio_params, &dec_opts) {
            Ok(d) => d,
            Err(e) => {
                error!("decode: decoder make failed: {e}");
                events.send(PlayerEvent::Error {
                    message: format!("decoder make failed: {e}"),
                });
                return;
            }
        };

        // Enable seek on the underlying source now that decoder is ready.
        controller.enable_seek_gate();
        trace!("decode: seek enabled, entering loop");

        // Temporary interleaved buffer for converted f32 samples.
        let mut tmp: Vec<f32> = Vec::new();
        let mut signalled_format = false;

        // Throttle for BufferLevel events (every ~500ms).
        let mut last_level_instant = std::time::Instant::now();

        // HLS-specific heuristic: retry on unexpected EOFs.
        let mut eof_retry_count: u32 = 0;

        // Decode loop: read packets, decode, convert to f32, push into ring buffer.
        loop {
            match format.next_packet() {
                Ok(Some(packet)) => {
                    if packet.track_id() != track_id {
                        // Attempt to (re)create decoder for the new track id (variant switch).
                        let new_tid = packet.track_id();
                        if let Some(t) = format.tracks().iter().find(|t| t.id == new_tid) {
                            if let Some(ap) =
                                t.codec_params.as_ref().and_then(|cp| cp.audio()).cloned()
                            {
                                match get_codecs().make_audio_decoder(&ap, &dec_opts) {
                                    Ok(d) => {
                                        decoder = d;
                                        track_id = new_tid;
                                        signalled_format = false; // force re-emit FormatChanged on first decoded frame
                                        trace!("decode: rebuilt decoder for track id={}", track_id);
                                    }
                                    Err(e) => {
                                        error!(
                                            "decode: failed to rebuild decoder for new track: {e}"
                                        );
                                        continue;
                                    }
                                }
                            } else {
                                // Unknown new track type; skip packet.
                                continue;
                            }
                        } else {
                            // New track id not present; skip packet.
                            continue;
                        }
                    }

                    match decoder.decode(&packet) {
                        Ok(decoded) => {
                            // Convert to a generic buffer reference and then to interleaved f32.
                            let gab: GenericAudioBufferRef = decoded;
                            let chans = gab.num_planes();
                            let frames = gab.frames();
                            let needed = chans * frames;

                            // Reset EOF retry counter on successful decode (HLS).
                            eof_retry_count = 0;

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
                                *spec_out.lock().unwrap() = new_spec;
                                trace!(
                                    "decode: first frame -> FormatChanged rate={}Hz ch={}",
                                    new_spec.sample_rate, new_spec.channels
                                );
                                events.send(PlayerEvent::FormatChanged {
                                    sample_rate: new_spec.sample_rate,
                                    channels: new_spec.channels,
                                    codec: None,
                                    container: container_name.clone(),
                                });
                                events.send(PlayerEvent::Started);
                                signalled_format = true;
                            }

                            // Push into the ring buffer with simple backpressure (no drops).
                            if needed > 0 {
                                if let Ok(mut prod) = producer.lock() {
                                    let mut i = 0usize;
                                    while i < needed {
                                        if prod.try_push(tmp[i]).is_ok() {
                                            i += 1;
                                        } else {
                                            // Ring full: wait for consumer to drain a bit.
                                            std::thread::sleep(std::time::Duration::from_millis(5));
                                        }
                                    }
                                }
                            }

                            // Throttle BufferLevel events (~500ms).
                            if last_level_instant.elapsed() >= std::time::Duration::from_millis(500)
                            {
                                events.send(PlayerEvent::BufferLevel {
                                    decoded_frames: frames,
                                });
                                last_level_instant = std::time::Instant::now();
                            }
                        }
                        Err(e) => {
                            // Non-fatal decode error; continue.
                            debug!("decode: decode error: {e}");
                        }
                    }
                }
                Ok(None) => {
                    // End of stream reached.
                    info!("decode: end of stream");
                    events.send(PlayerEvent::EndOfStream);
                    break;
                }
                Err(e) => {
                    let msg = e.to_string();
                    // Heuristic handling for segmented fMP4 (isomp4) over streaming (HLS):
                    // - Unexpected EOF while more data is expected: backoff and retry.
                    if msg.to_ascii_lowercase().contains("unexpected")
                        && msg.to_ascii_lowercase().contains("eof")
                    {
                        eof_retry_count = eof_retry_count.saturating_add(1);
                        if eof_retry_count > 50 {
                            info!("decode: repeated unexpected EOFs -> EndOfStream");
                            events.send(PlayerEvent::EndOfStream);
                            break;
                        }
                        debug!("decode: unexpected EOF (retry #{})", eof_retry_count);
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        continue;
                    }

                    error!("decode: next_packet error: {msg}");
                    events.send(PlayerEvent::Error {
                        message: format!("next_packet error: {msg}"),
                    });
                    break;
                }
            }
        }
    });
}

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
    /// This path opens the HLS MediaSourceStream, then starts a unified decode worker.
    pub async fn from_hls(
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
        let producer = Arc::new(Mutex::new(prod));

        // Clone state for unified decode worker.
        let events_clone = events.clone();
        let spec_clone = spec.clone();
        let producer_clone = producer.clone();
        let hls_cfg = hls_config.clone();
        let initial_variant_idx = match opts.initial_mode {
            crate::api::VariantMode::Manual(i) => Some(i),
            _ => None,
        };

        // Open HLS MediaSourceStream (spawns its own async producer internally).
        tracing::trace!("HLS backend: opening MediaSourceStream for URL: {}", url);
        let (mss, controller) = match crate::backends::hls::open_hls_media_source_async(
            url.clone(),
            hls_cfg,
            abr_config.clone(),
            initial_variant_idx,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                error!("HLS backend: open failed: {e}");
                events_clone.send(PlayerEvent::Error {
                    message: format!("HLS open failed: {e}"),
                });
                return Self {
                    spec,
                    producer,
                    consumer: cons,
                    events,
                    backend: Backend::Hls {
                        url,
                        hls_config,
                        abr_config,
                    },
                    processors: Arc::new(Mutex::new(Vec::new())),
                };
            }
        };

        // Probe and open the container in a blocking thread so we don't block the Tokio workers.
        // This allows the HLS producer task to run and feed bytes into the channel for probing.
        let hint = Hint::new();
        let probe_join = tokio::task::spawn_blocking(move || {
            get_probe().probe(
                &hint,
                mss,
                FormatOptions::default(),
                MetadataOptions::default(),
            )
        })
        .await;

        let format = match probe_join {
            Ok(Ok(f)) => f,
            Ok(Err(e)) => {
                error!("HLS backend: probe failed: {e}");
                events_clone.send(PlayerEvent::Error {
                    message: format!("probe failed: {e}"),
                });
                return Self {
                    spec,
                    producer,
                    consumer: cons,
                    events,
                    backend: Backend::Hls {
                        url,
                        hls_config,
                        abr_config,
                    },
                    processors: Arc::new(Mutex::new(Vec::new())),
                };
            }
            Err(join_err) => {
                error!("HLS backend: probe join error: {join_err}");
                events_clone.send(PlayerEvent::Error {
                    message: format!("probe join error: {join_err}"),
                });
                return Self {
                    spec,
                    producer,
                    consumer: cons,
                    events,
                    backend: Backend::Hls {
                        url,
                        hls_config,
                        abr_config,
                    },
                    processors: Arc::new(Mutex::new(Vec::new())),
                };
            }
        };

        trace!(
            "HLS backend: probe OK, format='{}'",
            format.format_info().short_name
        );
        let container_name = Some(format.format_info().short_name.to_string());

        // Start unified decode worker (spawn_blocking).
        spawn_decode_worker(
            format,
            controller,
            producer_clone,
            events_clone,
            spec_clone,
            container_name,
        );

        Self {
            spec,
            producer,
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
    /// This path opens the HTTP MediaSourceStream, then starts a unified decode worker.
    pub async fn from_http(url: impl Into<String>, opts: AudioOptions) -> Self {
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

        // Clone state for unified decode worker.
        let events_clone = events.clone();
        let spec_clone = spec.clone();
        let producer_clone = producer.clone();

        // Open a seek-gated MediaSourceStream.
        trace!("HTTP backend: opening seek-gated MSS for URL: {}", url);
        let (mss, gate) = match open_http_seek_gated_mss(&url).await {
            Ok(v) => v,
            Err(e) => {
                error!("HTTP backend: open failed: {e}");
                events_clone.send(PlayerEvent::Error {
                    message: format!("open failed: {e}"),
                });
                return Self {
                    spec,
                    producer,
                    consumer: cons,
                    events,
                    backend: Backend::Http { url },
                    processors: Arc::new(Mutex::new(Vec::new())),
                };
            }
        };

        // Probe and open the container in a blocking thread so we don't block the Tokio workers.
        // This avoids starving the HTTP fetcher and lets it feed bytes for probing.
        let mut hint = Hint::new();
        if let Some(ext) = extension_from_url(&url) {
            hint.with_extension(&ext);
        }
        let probe_join = tokio::task::spawn_blocking(move || {
            get_probe().probe(
                &hint,
                mss,
                FormatOptions::default(),
                MetadataOptions::default(),
            )
        })
        .await;

        let format = match probe_join {
            Ok(Ok(f)) => f,
            Ok(Err(e)) => {
                error!("HTTP backend: probe failed: {e}");
                events_clone.send(PlayerEvent::Error {
                    message: format!("probe failed: {e}"),
                });
                return Self {
                    spec,
                    producer,
                    consumer: cons,
                    events,
                    backend: Backend::Http { url },
                    processors: Arc::new(Mutex::new(Vec::new())),
                };
            }
            Err(join_err) => {
                error!("HTTP backend: probe join error: {join_err}");
                events_clone.send(PlayerEvent::Error {
                    message: format!("probe join error: {join_err}"),
                });
                return Self {
                    spec,
                    producer,
                    consumer: cons,
                    events,
                    backend: Backend::Http { url },
                    processors: Arc::new(Mutex::new(Vec::new())),
                };
            }
        };

        trace!(
            "HTTP backend: probe OK, format='{}'",
            format.format_info().short_name
        );
        let container_name = Some(format.format_info().short_name.to_string());

        // Enable seeking now that the decoder is initialized (in worker).
        // We pass the gate as controller to the unified worker which calls enable_seek().
        spawn_decode_worker(
            format,
            gate,
            producer_clone,
            events_clone,
            spec_clone,
            container_name,
        );

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
