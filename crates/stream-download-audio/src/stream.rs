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
use tracing::trace;

use crate::api::{AudioOptions, AudioProcessor, AudioSpec, FloatSampleSource, PlayerEvent};
use stream_download_hls::{AbrConfig, HlsConfig};

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
    producer: HeapProd<f32>,
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
            producer: prod,
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

        Self {
            spec,
            producer: prod,
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
            if self.producer.try_push(s).is_err() {
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
