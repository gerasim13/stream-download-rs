/*!
Adaptive audio module: FloatSampleSource trait and AdaptiveHlsAudio skeleton.

Overview:
- Provides a minimal, player-oriented trait `FloatSampleSource` to pull interleaved f32 PCM.
- Adds a skeleton `AdaptiveHlsAudio` that can:
  - Initialize HLS + ABR (via HlsManager + AbrController).
  - Spawn background tasks (Tokio runtime) to fetch segments.
  - Provide decoded PCM through a thread-safe ring buffer.
  - Broadcast player events through a simple hub (crossbeam-channel recommended).

Notes:
- This is a skeleton intended to be fleshed out iteratively.
- Actual demux/decode is not implemented yet; the background task currently
  synthesizes silence based on incoming segment durations.
- Symphonia decoding scaffolding is included behind feature flags for future work.

Feature gates:
- "adaptive-audio": enables the background pipeline and crossbeam-channel event hub.
- "symphonia": enables the Symphonia scaffolding for decoding (not wired yet).

Until Cargo.toml declares these optional dependencies and features, this module remains
standalone and won’t participate in the build (not referenced from lib.rs).
When wiring this module, add `mod audio;` in lib.rs and update Cargo.toml accordingly.

*/

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::traits::MediaStream;
use crate::{
    AbrConfig, AbrController, DownloaderConfig, HlsConfig, HlsError, HlsManager, ResourceDownloader,
};

#[cfg(feature = "adaptive-audio")]
use crossbeam_channel as xchan;

#[cfg(all(feature = "adaptive-audio", feature = "symphonia"))]
mod symphonia_support {
    // Skeleton-only imports; actual usage will be implemented later.
    use symphonia::core::audio::{AudioBufferRef, Signal, SignalSpec};
    use symphonia::core::codecs::{CodecParameters, Decoder, DecoderOptions};
    use symphonia::core::formats::{FormatOptions, FormatReader, Packet, Track};
    use symphonia::core::io::MediaSourceStream;
    use symphonia::core::meta::MetadataOptions;
    use symphonia::core::probe::ProbeResult;

    pub struct DecoderState {
        #[allow(dead_code)]
        pub format: Box<dyn FormatReader>,
        #[allow(dead_code)]
        pub decoder: Box<dyn Decoder>,
        #[allow(dead_code)]
        pub track_id: u32,
        #[allow(dead_code)]
        pub signal_spec: SignalSpec,
        #[allow(dead_code)]
        pub params: CodecParameters,
    }

    impl DecoderState {
        #[allow(unused)]
        pub fn new_dummy() -> Self {
            // Dummy values for skeleton only
            Self {
                format: unreachable!("decoder not implemented yet"),
                decoder: unreachable!("decoder not implemented yet"),
                track_id: 0,
                signal_spec: SignalSpec::new(48000, 2),
                params: CodecParameters::default(),
            }
        }
    }
}

/// Basic static PCM spec for the current decoded stream.
#[derive(Clone, Copy, Debug)]
pub struct AudioSpec {
    pub sample_rate: u32,
    pub channels: u16,
}

/// Minimal standard source of f32 interleaved samples.
///
/// read_interleaved may block until at least one sample is available, unless the
/// source is at EOF.
pub trait FloatSampleSource: Send {
    /// Fill `out` with interleaved f32 samples. Returns the number of f32 written.
    /// Implementations may block until data becomes available.
    fn read_interleaved(&mut self, out: &mut [f32]) -> usize;

    /// Current PCM spec.
    fn spec(&self) -> AudioSpec;

    /// Optional seek (for VOD). Default: unsupported.
    fn seek(&mut self, _to: Duration) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "seek not supported",
        ))
    }

    /// Indicates whether the source has no more data to produce.
    fn is_eof(&self) -> bool {
        false
    }
}

/// Mode for selecting HLS variant.
#[derive(Clone, Copy, Debug)]
pub enum VariantMode {
    Auto,
    /// Index into the variants returned by the master playlist.
    Manual(usize),
}

/// Options for `AdaptiveHlsAudio`.
#[derive(Clone, Debug)]
pub struct AdaptiveHlsAudioOptions {
    pub initial_mode: VariantMode,
    /// Target decoded buffer size in milliseconds (soft target).
    pub target_decoded_ms: u32,
    /// Target compressed buffer size in milliseconds (soft target).
    pub target_compressed_ms: u32,
    /// Min time between ABR switches.
    pub abr_min_switch_interval_ms: u32,
    /// Hysteresis for up-switch decisions.
    pub abr_hysteresis_ratio: f32,
    /// Try to prefer audio-only variants in master.
    pub prefer_audio_only: bool,
}

impl Default for AdaptiveHlsAudioOptions {
    fn default() -> Self {
        Self {
            initial_mode: VariantMode::Auto,
            target_decoded_ms: 500,
            target_compressed_ms: 1500,
            abr_min_switch_interval_ms: 4000,
            abr_hysteresis_ratio: 0.15,
            prefer_audio_only: true,
        }
    }
}

/// Playback-level events to improve observability.
#[derive(Clone, Debug)]
pub enum PlayerEvent {
    Started,
    VariantSwitched {
        from: Option<usize>,
        to: usize,
        reason: String,
    },
    FormatChanged {
        sample_rate: u32,
        channels: u16,
        codec: Option<String>,
        container: Option<String>,
    },
    BufferLevel {
        decoded_frames: usize,
    },
    EndOfStream,
    Error {
        message: String,
    },
}

/// Simple ring buffer for decoded samples (interleaved f32).
struct DecodedSampleRing {
    inner: Mutex<DecodedInner>,
    cv: Condvar,
    capacity_frames: usize, // capacity in frames, not f32 count; channels multiply applies.
    channels: u16,
}

struct DecodedInner {
    buf: VecDeque<f32>,
    closed: bool,
}

impl DecodedSampleRing {
    fn new(capacity_frames: usize, channels: u16) -> Self {
        Self {
            inner: Mutex::new(DecodedInner {
                buf: VecDeque::with_capacity(capacity_frames * channels as usize),
                closed: false,
            }),
            cv: Condvar::new(),
            capacity_frames,
            channels,
        }
    }

    /// Push new interleaved f32 samples. If overflow, drop old data (oldest).
    fn push_samples(&self, samples: &[f32]) {
        let mut inner = self.inner.lock().unwrap();
        let cap_f32 = self.capacity_frames * self.channels as usize;

        // If push would overflow, drop oldest
        let needed = samples.len();
        let available = cap_f32.saturating_sub(inner.buf.len());
        if available < needed {
            let drop_count = needed - available;
            for _ in 0..drop_count {
                inner.buf.pop_front();
            }
        }
        // Push
        inner.buf.extend(samples);
        // Notify waiters
        self.cv.notify_all();
    }

    /// Blocking pop into `out` returning number of f32 written.
    fn pop_interleaved(&self, out: &mut [f32]) -> usize {
        let mut inner = self.inner.lock().unwrap();
        loop {
            if inner.closed && inner.buf.is_empty() {
                return 0;
            }
            if !inner.buf.is_empty() {
                break;
            }
            inner = self.cv.wait(inner).unwrap();
        }

        let n = out.len().min(inner.buf.len());
        for i in 0..n {
            if let Some(s) = inner.buf.pop_front() {
                out[i] = s;
            } else {
                return i;
            }
        }
        n
    }

    fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.closed = true;
        self.cv.notify_all();
    }

    fn frames_available(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.buf.len() / self.channels as usize
    }
}

/// Control commands for the background pipeline.
#[derive(Debug)]
enum ControlCmd {
    SetMode(VariantMode),
    Stop,
}

/// A small event hub to broadcast PlayerEvent to multiple subscribers.
#[cfg(feature = "adaptive-audio")]
struct EventHub {
    subs: Mutex<Vec<xchan::Sender<PlayerEvent>>>,
}

#[cfg(feature = "adaptive-audio")]
impl EventHub {
    fn new() -> Self {
        Self {
            subs: Mutex::new(Vec::new()),
        }
    }

    fn subscribe(&self) -> xchan::Receiver<PlayerEvent> {
        let (tx, rx) = xchan::unbounded::<PlayerEvent>();
        self.subs.lock().unwrap().push(tx);
        rx
    }

    fn send(&self, ev: PlayerEvent) {
        let subs = self.subs.lock().unwrap();
        for tx in subs.iter() {
            let _ = tx.send(ev.clone());
        }
    }
}

/// The main public object that implements FloatSampleSource (interleaved f32).
///
/// Start with `new(url, opts)`, then `start()`. Read samples via `read_interleaved`.
/// You can switch ABR mode via `set_mode`.
pub struct AdaptiveHlsAudio {
    // Publicly observable spec; updated when we detect a format change.
    spec: Arc<Mutex<AudioSpec>>,

    // Decoded PCM buffer
    decoded: Arc<DecodedSampleRing>,

    // Control and lifecycle
    #[cfg(feature = "adaptive-audio")]
    ctrl_tx: xchan::Sender<ControlCmd>,
    #[cfg(feature = "adaptive-audio")]
    ctrl_rx: xchan::Receiver<ControlCmd>,
    #[cfg(feature = "adaptive-audio")]
    events: Arc<EventHub>,

    // Pipeline context (moved into background task on start)
    url: String,
    opts: AdaptiveHlsAudioOptions,

    // ABR/HLS components prepared in `new()` and moved at `start()`
    // Keep them in Option so we can take ownership.
    hls_config: HlsConfig,
    abr_config: AbrConfig,
    init_variant_index: usize,
}

impl AdaptiveHlsAudio {
    /// Creates a new adaptive audio pipeline for the given HLS master URL.
    pub fn new(url: impl Into<String>, opts: AdaptiveHlsAudioOptions) -> Result<Self, HlsError> {
        let url = url.into();

        // Defaults for initial spec: will be updated by decoder later.
        let spec = Arc::new(Mutex::new(AudioSpec {
            sample_rate: 48000,
            channels: 2,
        }));

        let decoded = Arc::new(DecodedSampleRing::new(
            // convert ms -> frames for capacity
            opts.target_decoded_ms as usize * 48, /* ~48 frames/ms at 48kHz */
            2,
        ));

        let hls_config = HlsConfig {
            live_refresh_interval: None,
            ..Default::default()
        };
        let abr_config = {
            let mut cfg = AbrConfig::default();
            cfg.min_switch_interval = Duration::from_millis(opts.abr_min_switch_interval_ms as u64);
            cfg.up_hysteresis_ratio = opts.abr_hysteresis_ratio;
            cfg
        };

        #[cfg(feature = "adaptive-audio")]
        let (ctrl_tx, ctrl_rx) = xchan::unbounded::<ControlCmd>();

        Ok(Self {
            spec,
            decoded,
            #[cfg(feature = "adaptive-audio")]
            ctrl_tx,
            #[cfg(feature = "adaptive-audio")]
            ctrl_rx,
            #[cfg(feature = "adaptive-audio")]
            events: Arc::new(EventHub::new()),
            url,
            opts,
            hls_config,
            abr_config,
            init_variant_index: 0,
        })
    }

    /// Starts the background pipeline (downloader + decoder).
    ///
    /// Skeleton implementation:
    /// - initializes ABR/HLS,
    /// - pulls segments and synthesizes silence into decoded buffer,
    /// - broadcasts events.
    ///
    /// Future: wire demux/decode with Symphonia to push real PCM.
    pub fn start(&mut self) -> Result<(), HlsError> {
        #[cfg(feature = "adaptive-audio")]
        {
            let url = self.url.clone();
            let opts = self.opts.clone();
            let spec = self.spec.clone();
            let decoded = self.decoded.clone();

            let events = self.events.clone();
            let ctrl_rx = self.ctrl_rx.clone();

            let hls_config = self.hls_config.clone();
            let abr_config = self.abr_config.clone();
            let initial_idx = self.init_variant_index;

            // Spawn a dedicated thread with a Tokio runtime.
            std::thread::spawn(move || {
                let rt = match tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .thread_name("adaptive-hls-audio")
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        events.send(PlayerEvent::Error {
                            message: format!("tokio runtime build failed: {e}"),
                        });
                        decoded.close();
                        return;
                    }
                };

                rt.block_on(async move {
                    let downloader = ResourceDownloader::new(DownloaderConfig::default());
                    let manager = HlsManager::new(url, hls_config.clone(), downloader);
                    let mut abr = AbrController::new(
                        manager,
                        abr_config.clone(),
                        initial_idx,
                        2_000_000.0, // initial bandwidth estimate
                    );

                    // Init HLS + ABR
                    if let Err(e) = abr.init().await {
                        events.send(PlayerEvent::Error {
                            message: format!("init failed: {e:?}"),
                        });
                        decoded.close();
                        return;
                    }

                    // Apply initial mode
                    match opts.initial_mode {
                        VariantMode::Auto => {
                            abr.set_auto();
                        }
                        VariantMode::Manual(i) => {
                            let target_id_opt = abr.variants().get(i).map(|v| v.id);
                            if let Some(target_id) = target_id_opt {
                                if let Err(e) = abr.set_manual(target_id).await {
                                    events.send(PlayerEvent::Error {
                                        message: format!("manual select failed: {e:?}"),
                                    });
                                } else {
                                    events.send(PlayerEvent::VariantSwitched {
                                        from: None,
                                        to: target_id.0,
                                        reason: "initial manual".into(),
                                    });
                                }
                            }
                        }
                    }
                    events.send(PlayerEvent::Started);

                    // Main loop: fetch segments and synthesize PCM silence as placeholder.
                    // Also listen for control commands (non-blocking poll).
                    loop {
                        // Drain control commands if any
                        while let Ok(cmd) = ctrl_rx.try_recv() {
                            match cmd {
                                ControlCmd::SetMode(m) => match m {
                                    VariantMode::Auto => abr.set_auto(),
                                    VariantMode::Manual(i) => {
                                        let target_id_opt = abr.variants().get(i).map(|v| v.id);
                                        if let Some(target_id) = target_id_opt {
                                            if let Err(e) = abr.set_manual(target_id).await {
                                                events.send(PlayerEvent::Error {
                                                    message: format!("manual select failed: {e:?}"),
                                                });
                                            } else {
                                                events.send(PlayerEvent::VariantSwitched {
                                                    from: abr.current_variant_id().map(|id| id.0),
                                                    to: target_id.0,
                                                    reason: "manual command".into(),
                                                });
                                            }
                                        }
                                    }
                                },
                                ControlCmd::Stop => {
                                    decoded.close();
                                    events.send(PlayerEvent::EndOfStream);
                                    return;
                                }
                            }
                        }

                        match abr.next_segment().await {
                            Ok(Some(seg)) => {
                                // TODO: On first segment or variant/container change:
                                // - download and feed init segment for fMP4 via manager.
                                // - (future) reinitialize symphonia demux/decoder.

                                // Placeholder "decode": generate zeros matching duration (in frames).
                                let s = spec.lock().unwrap().clone();
                                let frames = (seg.duration.as_secs_f64() * s.sample_rate as f64)
                                    .ceil() as usize;
                                let samples = frames * s.channels as usize;

                                // Keep zeros; could add a fade-in/out to show progress.
                                let silence = vec![0.0f32; samples];
                                decoded.push_samples(&silence);

                                // Monitoring
                                events.send(PlayerEvent::BufferLevel {
                                    decoded_frames: decoded.frames_available(),
                                });
                            }
                            Ok(None) => {
                                decoded.close();
                                events.send(PlayerEvent::EndOfStream);
                                break;
                            }
                            Err(e) => {
                                events.send(PlayerEvent::Error {
                                    message: format!("segment error: {e:?}"),
                                });
                                // Backoff a bit in live scenarios.
                                tokio::time::sleep(Duration::from_millis(250)).await;
                                continue;
                            }
                        }
                    }
                });
            });

            Ok(())
        }

        #[cfg(not(feature = "adaptive-audio"))]
        {
            Err(HlsError::msg("adaptive-audio feature disabled"))
        }
    }

    /// Switch selection mode at runtime.
    pub fn set_mode(&self, mode: VariantMode) {
        #[cfg(feature = "adaptive-audio")]
        {
            let _ = self.ctrl_tx.send(ControlCmd::SetMode(mode));
        }
    }

    /// Subscribe to player events.
    #[cfg(feature = "adaptive-audio")]
    pub fn subscribe_events(&self) -> xchan::Receiver<PlayerEvent> {
        self.events.subscribe()
    }

    /// Request shutdown.
    pub fn stop(&self) {
        #[cfg(feature = "adaptive-audio")]
        {
            let _ = self.ctrl_tx.send(ControlCmd::Stop);
        }
        self.decoded.close();
    }
}

impl FloatSampleSource for AdaptiveHlsAudio {
    fn read_interleaved(&mut self, out: &mut [f32]) -> usize {
        self.decoded.pop_interleaved(out)
    }

    fn spec(&self) -> AudioSpec {
        *self.spec.lock().unwrap()
    }
}

// Optional: Drop to ensure ring closure.
impl Drop for AdaptiveHlsAudio {
    fn drop(&mut self) {
        self.decoded.close();
    }
}
