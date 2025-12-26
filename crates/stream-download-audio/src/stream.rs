use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_util::Stream;
use kanal;
use stream_download::storage::StorageProvider;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::trace;
use url::Url;

use crate::api::{AudioMsg, AudioOptions, AudioProcessor, AudioSpec, PlayerEvent};
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

/// High-level audio stream.
///
/// NOTE: legacy pull-based `SampleSource` wiring is intentionally removed.
/// The primary consumption API is an ordered stream (see `api.rs`), and this type is driven by a
/// background pipeline (I/O, decode, resample, effects).
///
/// Generic over `StorageProvider` to allow flexible storage backends.
/// Note: HLS backend currently manages its own storage internally.
pub struct AudioStream<P: StorageProvider> {
    spec: Arc<Mutex<AudioSpec>>,
    events: Arc<EventHub>,
    /// Ordered decoded output (frames + control). This is the primary consumption channel.
    ///
    /// NOTE: `tokio::sync::mpsc::Receiver` is not clonable; consumers should have exactly one owner.
    msg_rx: mpsc::Receiver<AudioMsg>,
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
        on_producer_event: Option<Arc<dyn Fn(PlayerEvent) + Send + Sync>>,
    ) -> Self {
        let spec = Arc::new(Mutex::new(AudioSpec {
            sample_rate: opts.target_sample_rate,
            channels: opts.target_channels,
        }));
        let events = Arc::new(EventHub::new());

        let byte_capacity = 8usize;
        // Bounded, ordered message buffer capacity.
        // Keep this relatively small to provide backpressure like `stream-download-hls`.
        let msg_capacity = opts
            .frame_buffer_capacity_frames
            .saturating_mul(opts.target_channels as usize)
            .max(1);

        let mut runner = PipelineRunner::new(byte_capacity, msg_capacity);
        let msg_rx = runner.msg_rx.take().expect("msg_rx already taken");

        // Wire events from pipeline to this AudioStream's EventHub.
        let events_clone = events.clone();
        runner.on_event = Some(Arc::new(move |ev| events_clone.send(ev)));

        // Allow the packet producer to emit player events too (e.g., HLS VariantChanged).
        //
        // If the caller didn't provide a producer callback, default to broadcasting via the same hub.
        let on_producer_event = on_producer_event.or_else(|| {
            let events_clone = events.clone();
            Some(Arc::new(move |ev: PlayerEvent| events_clone.send(ev))
                as Arc<dyn Fn(PlayerEvent) + Send + Sync>)
        });

        // Create command channel for seek support if enabled.
        //
        // NOTE: The legacy pull-based API (and seek wiring on `AudioStream`) was removed during the
        // ordered `AudioMsg` stream redesign. We still keep the producer-side command receiver so
        // the backend can support seek in the future, but we intentionally do not retain the
        // command sender in `AudioStream` for now.
        let (_command_tx, command_rx) = if enable_seek {
            let (_tx_async, rx_async) = kanal::unbounded_async::<ProducerCommand>();
            (Some(()), Some(rx_async))
        } else {
            (None, None)
        };

        // Launch packet producer (async) using the producer half from runner.
        //
        // If the producer supports emitting player events, it can use the provided callback.
        let byte_tx = runner.byte_tx.take().expect("byte producer already taken");
        tokio::spawn(async move {
            // Best-effort: if the producer doesn't use `on_producer_event`, nothing changes.
            if let Err(e) = producer
                .run(byte_tx, command_rx, cancel, on_producer_event.clone())
                .await
            {
                trace!("Packet producer error: {:?}", e);
            }

            // Keep `on_producer_event` alive for the duration of the task even if not used directly here.
            let _ = on_producer_event;
        });

        // Launch decoder loop (spawn_blocking internally)
        let processors = Arc::new(Mutex::new(Vec::<Arc<dyn AudioProcessor>>::new()));
        let _decoder = runner.spawn_decoder_loop(*spec.lock().unwrap(), processors);

        Self {
            spec,
            events,
            msg_rx,
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
        _stream_settings: stream_download::Settings<
            stream_download::http::HttpStream<reqwest::Client>,
        >,
    ) -> Self {
        // Convert AudioSettings to AudioOptions for now
        let opts = AudioOptions {
            selection_mode: crate::api::SelectionMode::Auto,
            target_sample_rate: audio_settings.target_sample_rate,
            target_channels: audio_settings.target_channels,
            frame_buffer_capacity_frames: audio_settings.ring_capacity_frames,
            abr_min_switch_interval: std::time::Duration::from_secs(4),
            abr_up_hysteresis_ratio: 0.15,
        };

        let producer = HttpPacketProducer::new(url);
        Self::from_packet_producer(producer, opts, None, true, None).await // enable_seek=true
    }

    /// Subscribe to player events.
    pub fn subscribe_events(&self) -> kanal::Receiver<PlayerEvent> {
        self.events.subscribe()
    }

    /// Convenience getter for the current output spec.
    pub fn output_spec(&self) -> AudioSpec {
        *self.spec.lock().unwrap()
    }

    /// Async receive of the next ordered audio message (frame/control).
    ///
    /// This is the recommended consumption API.
    pub async fn next_msg(&mut self) -> Option<AudioMsg> {
        self.msg_rx.recv().await
    }

    /// Non-blocking attempt to receive the next ordered audio message.
    pub fn try_recv(&mut self) -> Option<AudioMsg> {
        self.msg_rx.try_recv().ok()
    }
}

impl<P: StorageProvider> Stream for AudioStream<P> {
    type Item = AudioMsg;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY:
        // - `AudioStream` does not implement a custom `Drop` that could move fields after being pinned.
        // - We only project a pinned `&mut` to the `msg_rx` field to call `poll_recv`.
        //
        // If in the future `AudioStream` becomes `!Unpin` due to self-references, switch to a
        // proper pin-projection crate (like `pin-project`) and annotate the struct.
        unsafe { self.map_unchecked_mut(|s| &mut s.msg_rx) }.poll_recv(cx)
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
            frame_buffer_capacity_frames: audio_settings.ring_capacity_frames,
            abr_min_switch_interval: std::time::Duration::from_secs(4),
            abr_up_hysteresis_ratio: 0.15,
        };

        // HlsPacketProducer manages its own storage.
        //
        // Provide an event callback so the HLS layer can bubble up `PlayerEvent::VariantChanged`
        // (derived from `stream_download_hls::StreamEvent::VariantChanged`) to AudioStream observers.
        //
        // Important: we do NOT try to replace `EventHub` after construction. Instead, we pass a
        // producer-event callback into `from_packet_producer`, which will default to broadcasting
        // into the same hub.
        let producer = HlsPacketProducer::new(url, hls_settings, storage_root);

        Self::from_packet_producer(producer, opts, None, true, None).await // enable_seek=true
    }
}
