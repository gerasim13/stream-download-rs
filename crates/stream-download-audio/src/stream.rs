use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use kanal;
use stream_download::storage::StorageProvider;
use tokio_util::sync::CancellationToken;
use tracing::trace;
use url::Url;

use crate::api::{AudioOptions, AudioProcessor, AudioSpec, PlayerEvent, SampleSource};
use crate::backends::{HlsPacketProducer, HttpPacketProducer, PacketProducer, ProducerCommand};
use crate::pipeline::PipelineRunner;
use crate::settings::AudioSettings;

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
/// This type implements `SampleSource` and is intended to be fed by a background
/// pipeline (I/O, decode, resample, effects).
/// 
/// Generic over `StorageProvider` to allow flexible storage backends.
/// Note: HLS backend currently manages its own storage internally.
pub struct AudioStream<P: StorageProvider> {
    spec: Arc<Mutex<AudioSpec>>,
    runner: PipelineRunner,
    events: Arc<EventHub>,
    /// Command channel to send seek/flush commands to producer task (sync sender for use from SampleSource)
    command_tx: Option<kanal::Sender<ProducerCommand>>,
    _phantom: PhantomData<P>,
}

impl<P: StorageProvider> AudioStream<P> {
    /// Create an AudioStream from a generic packet producer.
    ///
    /// This is a unified constructor that works with any backend implementing
    /// the `PacketProducer` trait.
    pub async fn from_packet_producer(
        mut producer: impl PacketProducer + 'static,
        opts: AudioOptions,
        cancel: Option<CancellationToken>,
        enable_seek: bool,
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

        // Create command channel for seek support if enabled
        // Use async receiver (for producer task) and sync sender (for AudioStream::seek)
        let (command_tx, command_rx) = if enable_seek {
            let (tx_async, rx_async) = kanal::unbounded_async::<ProducerCommand>();
            let tx_sync = tx_async.clone_sync(); // Clone sync sender for use in seek()
            (Some(tx_sync), Some(rx_async))
        } else {
            (None, None)
        };

        // Launch packet producer (async) using the producer half from runner.
        let byte_tx = runner.byte_tx.take().expect("byte producer already taken");
        tokio::spawn(async move {
            if let Err(e) = producer.run(byte_tx, command_rx, cancel).await {
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
            command_tx,
            _phantom: PhantomData,
        }
    }

    /// Create an AudioStream from an HTTP URL (e.g., MP3/AAC/FLAC).
    ///
    /// # Arguments
    /// * `url` - HTTP URL to the audio resource
    /// * `storage_provider` - Storage backend for buffering
    /// * `audio_settings` - Audio pipeline settings (sample rate, channels, etc.)
    /// * `stream_settings` - Stream-download settings (prefetch, retry, etc.)
    pub async fn new_http(
        url: Url,
        _storage_provider: P,
        audio_settings: AudioSettings,
        _stream_settings: stream_download::Settings<stream_download::http::HttpStream<reqwest::Client>>,
    ) -> Self {
        // Convert AudioSettings to AudioOptions for now
        let opts = AudioOptions {
            selection_mode: crate::api::SelectionMode::Auto,
            target_sample_rate: audio_settings.target_sample_rate,
            target_channels: audio_settings.target_channels,
            ring_capacity_frames: audio_settings.ring_capacity_frames,
            abr_min_switch_interval: std::time::Duration::from_secs(4),
            abr_up_hysteresis_ratio: 0.15,
        };
        
        let producer = HttpPacketProducer::new(url);
        Self::from_packet_producer(producer, opts, None, true).await // enable_seek=true
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

    /// Clear the PCM buffer.
    /// 
    /// This is useful when seeking to avoid playing stale audio.
    pub fn clear_buffer(&mut self) {
        // Drain all samples from PCM ring buffer
        let mut dummy = vec![0.0f32; 1024];
        while self.runner.pop_chunk(&mut dummy) > 0 {}
    }
}

// HLS-specific impl block (uses dummy storage type since HLS manages its own storage)
impl AudioStream<stream_download::storage::temp::TempStorageProvider> {
    /// Create an AudioStream from an HLS master playlist URL.
    ///
    /// # Arguments
    /// * `url` - HLS master playlist URL
    /// * `storage_root` - Directory path for persistent HLS caching (playlists, keys, segments)
    /// * `audio_settings` - Audio pipeline settings (sample rate, channels, etc.)
    /// * `hls_settings` - HLS-specific settings (ABR, variant selection, etc.)
    /// 
    /// # Notes
    /// HLS requires persistent storage for caching playlists/keys. This is managed internally
    /// by HlsPacketProducer. If `storage_root` is None, a temp directory will be used.
    /// The storage provider type parameter is unused (TempStorageProvider is a dummy).
    pub async fn new_hls(
        url: Url,
        storage_root: Option<std::path::PathBuf>,
        audio_settings: AudioSettings,
        hls_settings: stream_download_hls::HlsSettings,
    ) -> Self {
        // Convert AudioSettings to AudioOptions for now
        let opts = AudioOptions {
            selection_mode: crate::api::SelectionMode::Auto,
            target_sample_rate: audio_settings.target_sample_rate,
            target_channels: audio_settings.target_channels,
            ring_capacity_frames: audio_settings.ring_capacity_frames,
            abr_min_switch_interval: std::time::Duration::from_secs(4),
            abr_up_hysteresis_ratio: 0.15,
        };
        
        // HlsPacketProducer manages its own storage
        let producer = HlsPacketProducer::new(url, hls_settings, storage_root);
        Self::from_packet_producer(producer, opts, None, true).await // enable_seek=true
    }
}

impl<P: StorageProvider> SampleSource for AudioStream<P> {
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

    fn seek(&mut self, to: std::time::Duration) -> std::io::Result<()> {
        // Clear PCM buffer first to avoid playing stale audio
        self.clear_buffer();

        // Send seek command to producer task if command channel is available
        if let Some(ref tx) = self.command_tx {
            // Use sync send - no block_on needed!
            tx.send(ProducerCommand::Seek(to))
                .map_err(|_| std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "failed to send seek command - producer task may have stopped"
                ))?;

            tracing::info!("Seek command sent: {:?}", to);
            Ok(())
        } else {
            // No command channel - seek not supported for this stream
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "seek not supported for this stream (command channel not initialized)",
            ))
        }
    }
}
