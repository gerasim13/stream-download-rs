use std::sync::{Arc, Mutex};

use kanal;
use tokio_util::sync::CancellationToken;
use tracing::trace;

use crate::api::{AudioOptions, AudioProcessor, AudioSpec, PlayerEvent, SampleSource};
use crate::backends::{HlsPacketProducer, HttpPacketProducer, PacketProducer};
use crate::pipeline::PipelineRunner;

/// Simple broadcast hub for `PlayerEvent` using `kanal`.
#[derive(Debug)]
pub(crate) struct EventHub {
    subs: Mutex<Vec<kanal::Sender<PlayerEvent>>>,
}

impl EventHub {
    pub fn new() -> Self {
        Self {
            subs: Mutex::new(Vec::new()),
        }
    }

    pub fn subscribe(&self) -> kanal::Receiver<PlayerEvent> {
        let (tx, rx) = kanal::unbounded::<PlayerEvent>();
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
    runner: PipelineRunner,
    events: Arc<EventHub>,
}

impl AudioStream {
    /// Create an AudioStream from a generic packet producer.
    ///
    /// This is a unified constructor that works with any backend implementing
    /// the `PacketProducer` trait.
    pub async fn from_packet_producer(
        mut producer: impl PacketProducer + 'static,
        opts: AudioOptions,
        cancel: Option<CancellationToken>,
    ) -> Self {
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

        // Launch packet producer (async) using the producer half from runner.
        let byte_tx = runner.byte_tx.take().expect("byte producer already taken");
        tokio::spawn(async move {
            if let Err(e) = producer.run(byte_tx, cancel).await {
                trace!("Packet producer error: {:?}", e);
            }
        });

        // Launch decoder loop (spawn_blocking internally)
        let processors = Arc::new(Mutex::new(Vec::<Arc<dyn AudioProcessor>>::new()));
        let _decoder = runner.spawn_decoder_loop(*spec.lock().unwrap(), processors);

        Self {
            spec,
            runner,
            events,
        }
    }

    /// Create an AudioStream from an HLS master playlist URL.
    ///
    /// This path opens the HLS MediaSourceStream, then starts a unified decode worker.
    pub async fn from_hls(
        url: impl Into<String>,
        opts: AudioOptions,
        hls_config: stream_download_hls::HlsConfig,
        abr_config: stream_download_hls::AbrConfig,
    ) -> Self {
        let producer = HlsPacketProducer::new(url, hls_config, abr_config, opts.selection_mode);
        Self::from_packet_producer(producer, opts, None).await
    }

    /// Create an AudioStream from a regular HTTP URL (e.g., MP3/AAC/FLAC).
    ///
    /// This path opens the HTTP MediaSourceStream, then starts a unified decode worker.
    pub async fn from_http(url: impl Into<String>, opts: AudioOptions) -> Self {
        let producer = HttpPacketProducer::new(url);
        Self::from_packet_producer(producer, opts, None).await
    }

    /// Subscribe to player events.
    pub fn subscribe_events(&self) -> kanal::Receiver<PlayerEvent> {
        self.events.subscribe()
    }

    /// Convenience getter for the current output spec.
    pub fn output_spec(&self) -> AudioSpec {
        *self.spec.lock().unwrap()
    }

    /// Internal: pop up to `out.len()` samples into `out`, returning the count.
    pub fn pop_chunk(&mut self, out: &mut [f32]) -> usize {
        self.runner.pop_chunk(out)
    }
}

impl SampleSource for AudioStream {
    fn read_interleaved(&mut self, out: &mut [f32]) -> usize {
        // Use synchronous pop since we're now using sync ringbuf
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
