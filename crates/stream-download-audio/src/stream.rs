use std::sync::{Arc, Mutex};

use async_ringbuf::{AsyncHeapCons, traits::*};
use kanal as kchan;
use tracing::trace;

use crate::api::{AudioOptions, AudioProcessor, AudioSpec, FloatSampleSource, PlayerEvent};
use crate::backends::hls::run_hls_packet_producer;
use crate::backends::http::run_http_packet_producer;
use crate::pipeline::PipelineRunner;
use stream_download_hls::{AbrConfig, HlsConfig};

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
    pcm_cons: AsyncHeapCons<f32>,
    events: Arc<EventHub>,
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
        let events = Arc::new(EventHub::new());
        let byte_capacity = 8usize;
        let pcm_capacity = opts
            .ring_capacity_frames
            .saturating_mul(opts.target_channels as usize)
            .max(1);
        let mut runner = PipelineRunner::new(byte_capacity, pcm_capacity);

        // Wire events from pipeline to this AudioStream's EventHub.
        let events_clone = events.clone();
        runner.on_event = Some(Arc::new(move |ev| events_clone.send(ev)));

        // Launch HLS packet producer (async) using the producer half from runner.
        let byte_prod = runner
            .byte_prod
            .take()
            .expect("byte producer already taken");
        let cancel = tokio_util::sync::CancellationToken::new();
        tokio::spawn(run_hls_packet_producer(
            url.clone(),
            hls_config.clone(),
            abr_config.clone(),
            opts.selection_mode,
            byte_prod,
            cancel.clone(),
        ));

        // Launch decoder loop (spawn_blocking internally)
        let processors = Arc::new(Mutex::new(Vec::<Arc<dyn AudioProcessor>>::new()));
        let _decoder = runner.spawn_decoder_loop(*spec.lock().unwrap(), processors);

        // Expose runner's PCM ring to AudioStream

        let pcm_cons = runner.pcm_cons.take().expect("pcm consumer already taken");

        Self {
            spec,
            pcm_cons,
            events,
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
        let events = Arc::new(EventHub::new());
        let byte_capacity = 8usize;
        let pcm_capacity = opts
            .ring_capacity_frames
            .saturating_mul(opts.target_channels as usize)
            .max(1);
        let mut runner = PipelineRunner::new(byte_capacity, pcm_capacity);

        // Wire events from pipeline to this AudioStream's EventHub.
        let events_clone = events.clone();
        runner.on_event = Some(Arc::new(move |ev| events_clone.send(ev)));

        // Launch HTTP packet producer (async) using the producer half from runner.
        let byte_prod = runner
            .byte_prod
            .take()
            .expect("byte producer already taken");
        let url_for_task = url.clone();
        tokio::spawn(async move {
            let _ = run_http_packet_producer(url_for_task.as_str(), byte_prod).await;
        });

        // Launch decoder loop (spawn_blocking internally)
        let processors = Arc::new(Mutex::new(Vec::<Arc<dyn AudioProcessor>>::new()));
        let _decoder = runner.spawn_decoder_loop(*spec.lock().unwrap(), processors);

        // Expose runner's PCM ring to AudioStream
        let pcm_cons = runner.pcm_cons.take().expect("pcm consumer already taken");

        Self {
            spec,
            pcm_cons,
            events,
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
    pub async fn push_samples_for_test(&mut self, _samples: &[f32]) -> usize {
        0
    }

impl SampleSource for AudioStream {
    // fn read_interleaved(&mut self, out: &mut [f32]) -> usize {
    //     // Best-effort async -> sync bridge using a local current-thread runtime when necessary.
    //     // This avoids creating a multi-threaded runtime inside audio callback paths.
    //     let n = if let Ok(handle) = tokio::runtime::Handle::try_current() {
    //         handle.block_on(self.pop_chunk_async(out))
    //     } else {
    //         let rt = tokio::runtime::Builder::new_current_thread()
    //             .enable_all()
    //             .build()
    //             .expect("audio local rt");
    //         rt.block_on(self.pop_chunk_async(out))
    //     };
    //     if n < out.len() {
    //         trace!("AudioStream underrun: requested {}, got {}", out.len(), n);
    //     }
    //     n
    // }

    /// Internal: pop up to `out.len()` samples into `out`, returning the count.
    pub async fn pop_chunk_async(&mut self, out: &mut [f32]) -> usize {
        let mut n = 0usize;
        while n < out.len() {
            match self.pcm_cons.pop().await {
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
        // Best-effort async -> sync bridge using a local current-thread runtime when necessary.
        // This avoids creating a multi-threaded runtime inside audio callback paths.
        let n = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.block_on(self.pop_chunk_async(out))
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("audio local rt");
            rt.block_on(self.pop_chunk_async(out))
        };
        if n < out.len() {
            trace!("AudioStream underrun: requested {}, got {}", out.len(), n);
        }
        n
    }

    fn spec(&self) -> AudioSpec {
        *self.spec.lock().unwrap()
    }
}
