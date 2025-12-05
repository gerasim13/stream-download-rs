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
use symphonia::core::formats::{FormatOptions, TrackType};
use symphonia::core::meta::MetadataOptions;
use symphonia::default::{get_codecs, get_probe};
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

        // Clone state for background decoding worker.
        let url_clone = url.clone();
        let events_clone = events.clone();
        let spec_clone = spec.clone();
        let producer_clone = producer.clone();
        let hls_cfg = hls_config.clone();
        let initial_variant_idx = match opts.initial_mode {
            crate::api::VariantMode::Manual(i) => Some(i),
            _ => None,
        };

        // Open HLS MediaSourceStream (spawns its own async producer internally).
        tracing::trace!(
            "HLS backend: opening MediaSourceStream for URL: {}",
            url_clone
        );
        let (mss, controller) = match crate::backends::hls::open_hls_media_source_async(
            url_clone.clone(),
            hls_cfg,
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

        // Spawn blocking decode worker so we don't block the async runtime.
        tokio::task::spawn_blocking(move || {
            // Use owned clone of URL inside blocking task to satisfy 'static lifetime.
            let _url_for_logs = url_clone.clone();
            tracing::trace!("HLS backend: MSS created, starting probe...");

            // No specific hint required; Symphonia should detect fMP4/ADTS from bytes.
            let hint = Hint::new();

            // Probe and open the container.
            let mut format = match get_probe().probe(
                &hint,
                mss,
                FormatOptions::default(),
                MetadataOptions::default(),
            ) {
                Ok(f) => f,
                Err(e) => {
                    error!("HLS backend: probe failed: {e}");
                    events_clone.send(PlayerEvent::Error {
                        message: format!("probe failed: {e}"),
                    });
                    return;
                }
            };
            trace!(
                "HLS backend: probe OK, format='{}'",
                format.format_info().short_name
            );

            let container_name = Some(format.format_info().short_name.to_string());
            trace!("HLS backend: selecting default audio track...");

            // Select the default audio track.
            let (track_id, owned_audio_params) = match format.default_track(TrackType::Audio) {
                Some(t) => match t.codec_params.as_ref().and_then(|cp| cp.audio()).cloned() {
                    Some(ap) => {
                        trace!(
                            "HLS backend: selected track id={} sr={:?} ch={:?}",
                            t.id,
                            ap.sample_rate,
                            ap.channels.clone().map(|c| c.count())
                        );
                        (t.id, ap)
                    }
                    None => {
                        error!("HLS backend: selected track is not audio");
                        events_clone.send(PlayerEvent::Error {
                            message: "no audio params on selected track".into(),
                        });
                        return;
                    }
                },
                None => {
                    error!("HLS backend: no default audio track");
                    events_clone.send(PlayerEvent::Error {
                        message: "no default audio track".into(),
                    });
                    return;
                }
            };

            // Build the decoder using dev-0.6 API with owned audio params.
            let dec_opts = AudioDecoderOptions::default();
            trace!("HLS backend: creating audio decoder...");
            let mut decoder = match get_codecs().make_audio_decoder(&owned_audio_params, &dec_opts)
            {
                Ok(d) => d,
                Err(e) => {
                    error!("HLS backend: decoder make failed: {e}");
                    events_clone.send(PlayerEvent::Error {
                        message: format!("decoder make failed: {e}"),
                    });
                    return;
                }
            };
            trace!("HLS backend: decoder created, enabling seek gate.");
            controller.enable_seek();

            let mut pkt_counter: u64 = 0;
            let mut total_decoded_frames: u64 = 0;
            trace!(
                "HLS backend: seek gate enabled, entering decode loop. pkt_counter={} total_frames={}",
                pkt_counter, total_decoded_frames
            );

            // Temporary interleaved buffer for converted f32 samples.
            let mut tmp: Vec<f32> = Vec::new();
            let mut signalled_format = false;
            // Count consecutive unexpected EOFs to treat VOD end as graceful EOF.
            let mut eof_retry_count: u32 = 0;

            // Decode loop: read packets, decode, convert to f32, push into ring buffer.
            loop {
                pkt_counter = pkt_counter.saturating_add(1);
                trace!("HLS backend: next_packet() call #{}", pkt_counter);
                match format.next_packet() {
                    Ok(Some(packet)) => {
                        if packet.track_id() != track_id {
                            continue;
                        }

                        match decoder.decode(&packet) {
                            Ok(decoded) => {
                                // Convert to a generic buffer reference and then to interleaved f32.
                                let gab: GenericAudioBufferRef = decoded;
                                // Reset EOF retry counter on successful decode.
                                eof_retry_count = 0;
                                let chans = gab.num_planes();
                                let frames = gab.frames();
                                let needed = chans * frames;
                                total_decoded_frames =
                                    total_decoded_frames.saturating_add(frames as u64);
                                trace!(
                                    "HLS backend: decoded packet #{} -> {} frames ({} ch), total_decoded_frames={}",
                                    pkt_counter, frames, chans, total_decoded_frames
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
                                    trace!(
                                        "HLS backend: first audio frame -> FormatChanged rate={}Hz ch={}",
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

                                // Push into the ring buffer with simple backpressure (no drops).
                                let mut pushed = 0usize;
                                if needed > 0 {
                                    if let Ok(mut prod) = producer_clone.lock() {
                                        let mut i = 0usize;
                                        while i < needed {
                                            if prod.try_push(tmp[i]).is_ok() {
                                                pushed += 1;
                                                i += 1;
                                            } else {
                                                // Ring full: wait for consumer to drain a bit.
                                                std::thread::sleep(
                                                    std::time::Duration::from_millis(5),
                                                );
                                            }
                                        }
                                    }
                                }

                                if pushed > 0 {
                                    trace!(
                                        "HLS backend: pushed {} samples to ring (~{} frames)",
                                        pushed,
                                        pushed / chans
                                    );
                                    events_clone.send(PlayerEvent::BufferLevel {
                                        decoded_frames: pushed / chans,
                                    });
                                }
                            }
                            Err(e) => {
                                // Non-fatal decode error; continue.
                                debug!("HLS backend: decode error: {e}");
                            }
                        }
                    }
                    Ok(None) => {
                        // End of stream reached.
                        info!("HLS backend: end of stream");
                        events_clone.send(PlayerEvent::EndOfStream);
                        break;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        // Heuristic handling for segmented fMP4 (isomp4) over streaming:
                        // - Unexpected EOF while more data is expected: backoff and retry.
                        // - Reset-required style errors: try to rebuild decoder from current track params.
                        if msg.contains("unexpected end of file")
                            || msg.contains("end of file")
                            || msg.contains("unexpected eof")
                        {
                            // Allow more bytes to arrive from the producer before retrying.
                            eof_retry_count = eof_retry_count.saturating_add(1);
                            if eof_retry_count > 50 {
                                // Treat repeated EOFs as graceful end-of-stream (VOD finished).
                                info!(
                                    "HLS backend: repeated unexpected EOFs, treating as EndOfStream"
                                );
                                events_clone.send(PlayerEvent::EndOfStream);
                                break;
                            }
                            debug!("HLS backend: unexpected EOF (retry #{})", eof_retry_count);
                            std::thread::sleep(std::time::Duration::from_millis(50));
                            continue;
                        } else if msg.to_ascii_lowercase().contains("reset")
                            || msg.contains("ResetRequired")
                        {
                            debug!(
                                "HLS backend: packet stream signalled reset; attempting to rebuild decoder"
                            );
                            // Try to get fresh audio params from the current default audio track.
                            if let Some(t) = format.default_track(TrackType::Audio) {
                                if let Some(ap) =
                                    t.codec_params.as_ref().and_then(|cp| cp.audio()).cloned()
                                {
                                    match get_codecs().make_audio_decoder(&ap, &dec_opts) {
                                        Ok(new_dec) => {
                                            decoder = new_dec;
                                            // Re-emit format on next decoded frame.
                                            signalled_format = false;
                                            debug!(
                                                "HLS backend: decoder successfully rebuilt after reset"
                                            );
                                            continue;
                                        }
                                        Err(e2) => {
                                            error!(
                                                "HLS backend: failed to rebuild decoder after reset: {e2}"
                                            );
                                        }
                                    }
                                } else {
                                    error!(
                                        "HLS backend: reset indicated but no audio params on selected track"
                                    );
                                }
                            } else {
                                error!(
                                    "HLS backend: reset indicated but no default audio track found"
                                );
                            }
                            events_clone.send(PlayerEvent::Error {
                                message: format!("decoder reset failed after error: {msg}"),
                            });
                            break;
                        } else {
                            error!("HLS backend: next_packet error: {msg}");
                            events_clone.send(PlayerEvent::Error {
                                message: format!("next_packet error: {msg}"),
                            });
                            break;
                        }
                    }
                }
            }
            // Note: we don't explicitly stop the HLS producer here; it will end on EOF or cancellation.
        });

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
    /// This skeleton only initializes state; background tasks are TODO.
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

        // Clone state for background decoding worker.
        let events_clone = events.clone();
        let spec_clone = spec.clone();
        let producer_clone = producer.clone();

        // Limit scope of borrowed URL so it doesn't live across the spawn_blocking move.
        let (mss, gate, url_for_logs_owned) = {
            let url_for_open: String = url.clone();
            let url_for_logs_owned = url.clone();

            // Open a seek-gated MediaSourceStream.
            trace!(
                "HTTP backend: opening seek-gated MSS for URL: {}",
                url_for_open
            );
            let (mss, gate) = match open_http_seek_gated_mss(&url_for_open).await {
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
            (mss, gate, url_for_logs_owned)
        };

        // Spawn blocking decode worker so we don't block the async runtime.
        tokio::task::spawn_blocking(move || {
            trace!("HTTP backend: MSS created, starting probe...");

            // Prepare a probe hint using the URL extension (if any).
            let mut hint = Hint::new();
            if let Some(ext) = extension_from_url(&url_for_logs_owned) {
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
            trace!(
                "HTTP backend: probe OK, format='{}'",
                format.format_info().short_name
            );

            let container_name = Some(format.format_info().short_name.to_string());
            trace!("HTTP backend: selecting default audio track...");

            // Select the default audio track.
            let (track_id, owned_audio_params) = match format.default_track(TrackType::Audio) {
                Some(t) => match t.codec_params.as_ref().and_then(|cp| cp.audio()).cloned() {
                    Some(ap) => {
                        trace!(
                            "HTTP backend: selected track id={} sr={:?} ch={:?}",
                            t.id,
                            ap.sample_rate,
                            ap.channels.clone().map(|c| c.count())
                        );
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
            trace!("HTTP backend: creating audio decoder...");
            let mut decoder = match get_codecs().make_audio_decoder(&owned_audio_params, &dec_opts)
            {
                Ok(d) => d,
                Err(e) => {
                    error!("HTTP backend: decoder make failed: {e}");
                    events_clone.send(PlayerEvent::Error {
                        message: format!("decoder make failed: {e}"),
                    });
                    return;
                }
            };
            trace!("HTTP backend: decoder created, enabling seek gate.");

            // Enable seeking now that the decoder is initialized.
            gate.enable_seek();
            trace!("HTTP backend: seek gate enabled, entering decode loop.");

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
                                trace!("HTTP backend: decoded {} frames ({} ch)", frames, chans);

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
                                    trace!(
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
                                                std::thread::sleep(
                                                    std::time::Duration::from_millis(5),
                                                );
                                            }
                                        }
                                    }
                                }

                                if pushed > 0 {
                                    trace!(
                                        "HTTP backend: pushed {} samples to ring (~{} frames)",
                                        pushed,
                                        pushed / chans
                                    );
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
