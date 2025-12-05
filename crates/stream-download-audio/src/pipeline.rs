/*!
Pipeline internals (skeleton)

This module is a placeholder for the future audio pipeline implementation. It defines
the structure and contracts for the background tasks that will:
- fetch bytes (from HLS segments or a single HTTP resource),
- decode compressed audio to interleaved f32 PCM,
- resample to the target audio session sample rate,
- apply an optional processing chain (effects),
- write samples into a lock-free ring buffer for consumption by the client (or rodio adapter).

Notes:
- Everything here is intentionally a stub. Methods are no-ops returning defaults.
- The final implementation will split this file into multiple modules:
  - source_hls:     HLS backend producing compressed bytes from segments.
  - source_http:    HTTP backend producing compressed bytes from a single resource.
  - decode:         Symphonia-based decode stage to PCM f32.
  - resample:       rubato-based resampler to the output spec.
  - process:        chain of AudioProcessor(s) applied post-resample.
  - buffers:        ring buffer wiring and helper utilities.

Until those are implemented, this module documents the intended API surface without
spawning background tasks or performing actual work.
*/

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::{
    AbrConfig, AudioOptions, AudioProcessor, AudioSpec, HlsConfig, PlayerEvent, VariantMode,
};

/// Backend configuration for the pipeline.
///
/// This enumerates the supported audio sources: HLS (with configs) or HTTP (single resource).
#[derive(Debug, Clone)]
pub enum BackendConfig {
    Hls {
        url: String,
        hls: HlsConfig,
        abr: AbrConfig,
        mode: VariantMode,
    },
    Http {
        url: String,
    },
}

/// High-level pipeline state machine (skeleton).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineState {
    Stopped,
    Starting,
    Running,
    Draining,
    Stopping,
    Error,
}

/// Runtime configuration resolved for the pipeline.
///
/// This reflects the output (target) audio session spec and buffering choices.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub output_spec: AudioSpec,
    pub ring_capacity_frames: usize,
    pub abr_min_switch_interval: Duration,
    pub abr_up_hysteresis_ratio: f32,
}

impl From<&AudioOptions> for PipelineConfig {
    fn from(opts: &AudioOptions) -> Self {
        Self {
            output_spec: AudioSpec {
                sample_rate: opts.target_sample_rate,
                channels: opts.target_channels,
            },
            ring_capacity_frames: opts.ring_capacity_frames,
            abr_min_switch_interval: opts.abr_min_switch_interval,
            abr_up_hysteresis_ratio: opts.abr_up_hysteresis_ratio,
        }
    }
}

/// Pipeline builder (skeleton).
///
/// Constructed with a backend and options, optionally configured with processors,
/// producing a `Pipeline` that can be started.
#[derive(Clone)]
pub struct PipelineBuilder {
    backend: BackendConfig,
    config: PipelineConfig,
    processors: Vec<Arc<dyn AudioProcessor>>,
}

impl PipelineBuilder {
    /// Create a builder for an HLS backend.
    pub fn hls(url: impl Into<String>, opts: AudioOptions, hls: HlsConfig, abr: AbrConfig) -> Self {
        Self {
            backend: BackendConfig::Hls {
                url: url.into(),
                hls,
                abr,
                mode: opts.initial_mode,
            },
            config: PipelineConfig::from(&opts),
            processors: Vec::new(),
        }
    }

    /// Create a builder for an HTTP backend.
    pub fn http(url: impl Into<String>, opts: AudioOptions) -> Self {
        Self {
            backend: BackendConfig::Http { url: url.into() },
            config: PipelineConfig::from(&opts),
            processors: Vec::new(),
        }
    }

    /// Override ring capacity (frames).
    pub fn ring_capacity_frames(mut self, capacity: usize) -> Self {
        self.config.ring_capacity_frames = capacity;
        self
    }

    /// Add an audio processor to the chain (executed post-resample).
    pub fn add_processor(mut self, p: Arc<dyn AudioProcessor>) -> Self {
        self.processors.push(p);
        self
    }

    /// Build a `Pipeline`. The returned pipeline is not started yet.
    pub fn build(self) -> Pipeline {
        Pipeline {
            backend: self.backend,
            config: self.config,
            processors: Arc::new(Mutex::new(self.processors)),
            state: Arc::new(Mutex::new(PipelineState::Stopped)),
            events: EventHub::new(),
        }
    }
}

/// Simple event hub (skeleton) for broadcasting PlayerEvent.
///
/// The final implementation will use `kanal` senders/receivers owned by the pipeline.
/// Here we keep a simple subscription model for future replacement.
#[derive(Clone)]
pub struct EventHub {
    inner: Arc<Mutex<Vec<crossbeam_like::Tx<PlayerEvent>>>>,
}

impl EventHub {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Subscribe to player events; returns a receiver (stubbed).
    pub fn subscribe(&self) -> crossbeam_like::Rx<PlayerEvent> {
        let (tx, rx) = crossbeam_like::unbounded();
        self.inner.lock().unwrap().push(tx);
        rx
    }

    /// Broadcast an event to all subscribers (no-op if none).
    pub fn send(&self, ev: PlayerEvent) {
        let subs = self.inner.lock().unwrap();
        for tx in subs.iter() {
            let _ = tx.send(ev.clone());
        }
    }
}

/// The audio pipeline (skeleton).
///
/// This object will orchestrate background tasks in the future:
/// - IO (HLS/HTTP),
/// - Decode (Symphonia),
/// - Resample (rubato),
/// - Process (AudioProcessor chain),
/// - Buffering (ringbuf),
/// and emit `PlayerEvent`s along the way.
///
/// In this skeleton, methods are no-ops to establish the API surface first.
#[derive(Clone)]
pub struct Pipeline {
    backend: BackendConfig,
    config: PipelineConfig,
    processors: Arc<Mutex<Vec<Arc<dyn AudioProcessor>>>>,
    state: Arc<Mutex<PipelineState>>,
    events: EventHub,
}

impl Pipeline {
    /// Current pipeline state (cheap).
    pub fn state(&self) -> PipelineState {
        *self.state.lock().unwrap()
    }

    /// Event hub for subscribing to `PlayerEvent`s.
    pub fn events(&self) -> &EventHub {
        &self.events
    }

    /// Start the pipeline (no-op skeleton).
    ///
    /// Future behavior:
    /// - spawn tokio tasks for IO/decode/resample/process,
    /// - send PlayerEvent::Started when all stages are ready.
    pub fn start(&self) {
        let mut st = self.state.lock().unwrap();
        if *st != PipelineState::Stopped {
            warn!("Pipeline.start() called but state is {:?}", *st);
            return;
        }
        *st = PipelineState::Starting;
        info!("Pipeline is starting (skeleton)");
        *st = PipelineState::Running;

        self.events.send(PlayerEvent::Started);
    }

    /// Stop the pipeline (no-op skeleton).
    ///
    /// Future behavior:
    /// - signal tasks to stop,
    /// - drain buffers,
    /// - join handles.
    pub fn stop(&self) {
        let mut st = self.state.lock().unwrap();
        match *st {
            PipelineState::Running | PipelineState::Starting | PipelineState::Draining => {
                *st = PipelineState::Stopping;
                info!("Pipeline is stopping (skeleton)");
                *st = PipelineState::Stopped;
                self.events.send(PlayerEvent::EndOfStream);
            }
            _ => debug!("Pipeline.stop() ignored; state={:?}", *st),
        }
    }

    /// Replace all processors (skeleton).
    pub fn set_processors(&self, new_list: Vec<Arc<dyn AudioProcessor>>) {
        let mut guard = self.processors.lock().unwrap();
        *guard = new_list;
    }

    /// Add a single processor to the end of the chain (skeleton).
    pub fn add_processor(&self, proc_: Arc<dyn AudioProcessor>) {
        let mut guard = self.processors.lock().unwrap();
        guard.push(proc_);
    }

    /// Clear the processor chain (skeleton).
    pub fn clear_processors(&self) {
        let mut guard = self.processors.lock().unwrap();
        guard.clear();
    }

    /// Change the output spec (e.g., after an audio session change).
    ///
    /// Future behavior:
    /// - re-create resampler,
    /// - update buffer sizes if needed,
    /// - emit FormatChanged.
    pub fn reconfigure_output(&self, new_spec: AudioSpec) {
        let mut cfg = self.config.clone();
        if cfg.output_spec != new_spec {
            cfg.output_spec = new_spec;
            self.events.send(PlayerEvent::FormatChanged {
                sample_rate: new_spec.sample_rate,
                channels: new_spec.channels,
                codec: None,
                container: None,
            });
        }
    }

    /// Return a copy of the current output spec.
    pub fn output_spec(&self) -> AudioSpec {
        self.config.output_spec
    }
}

/*------------------------------------------------------------------------------
 Placeholders for future submodules (will be split into files):

 - source_hls:     Mapping HlsManager + AbrController to a compressed byte stream.
 - source_http:    Mapping StreamDownload to a compressed byte stream.
 - decode:         Symphonia-based decoder to f32 interleaved PCM.
 - resample:       rubato-based resampler to target AudioSpec.
 - process:        AudioProcessor chain application.
 - buffers:        Ring buffer helpers / metrics.

 For now we keep them as empty modules to document the layout.
------------------------------------------------------------------------------*/

/// HLS source backend (placeholder).
pub mod source_hls {
    //! HLS source backend: connects `HlsManager` + `AbrController` to the decode stage.
    //!
    //! Responsibilities (future):
    //! - Handle init segments and segment fetching.
    //! - Align live sequences on variant switches.
    //! - Expose a `Stream<Item = Bytes>`-like abstraction for the decoder.

    /// Placeholder type for the future HLS source.
    #[derive(Debug)]
    pub struct HlsSource;
}

/// HTTP source backend (placeholder).
pub mod source_http {
    //! HTTP source backend: connects `stream-download` to the decode stage.
    //!
    //! Responsibilities (future):
    //! - Fetch single resource (streaming if possible).
    //! - Offer seek if the storage/backend allows it.

    /// Placeholder type for the future HTTP source.
    #[derive(Debug)]
    pub struct HttpSource;
}

/// Decode stage (placeholder).
pub mod decode {
    //! Decode compressed audio using Symphonia to interleaved f32 PCM.
    //!
    //! Responsibilities (future):
    //! - Format detection, track selection.
    //! - Expose a pull/push API for PCM frames.

    /// Placeholder type for the future decoder.
    #[derive(Debug)]
    pub struct Decoder;
}

/// Resample stage (placeholder).
pub mod resample {
    //! Resample interleaved f32 PCM to the target `AudioSpec` using `rubato`.
    //!
    //! Responsibilities (future):
    //! - Manage internal latency/buffers for high-quality resampling.
    //! - Recreate resampler on format/output changes.

    /// Placeholder type for the future resampler.
    #[derive(Debug)]
    pub struct Resampler;
}

/// Processing stage (placeholder).
pub mod process {
    //! Apply an `AudioProcessor` chain on interleaved f32 PCM.
    //!
    //! Responsibilities (future):
    //! - Execute processors in order.
    //! - Handle dynamic changes to the chain safely.

    /// Placeholder type for the future processor chain executor.
    #[derive(Debug)]
    pub struct ProcessorChain;
}

/// Buffer helpers (placeholder).
pub mod buffers {
    //! Ring buffer helpers and metrics around the final PCM queue.
    //!
    //! Responsibilities (future):
    //! - Encapsulate SPSC ring buffer logic.
    //! - Provide buffer level metrics/events.

    /// Placeholder type for the future PCM ring buffer wrapper.
    #[derive(Debug)]
    pub struct PcmRing;
}

/*------------------------------------------------------------------------------
 Minimal crossbeam-like stubs to allow the EventHub placeholder to compile
 without pulling actual channel implementations here.

 The real pipeline will use `kanal` senders/receivers instead of these.
------------------------------------------------------------------------------*/

mod crossbeam_like {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    pub struct Tx<T> {
        inner: Arc<Mutex<VecDeque<T>>>,
    }

    #[derive(Clone)]
    pub struct Rx<T> {
        inner: Arc<Mutex<VecDeque<T>>>,
    }

    pub fn unbounded<T>() -> (Tx<T>, Rx<T>) {
        let inner = Arc::new(Mutex::new(VecDeque::new()));
        (
            Tx {
                inner: inner.clone(),
            },
            Rx { inner },
        )
    }

    impl<T: Clone> Tx<T> {
        pub fn send(&self, item: T) -> Result<(), ()> {
            self.inner.lock().map_err(|_| ())?.push_back(item);
            Ok(())
        }
    }

    impl<T: Clone> Iterator for Rx<T> {
        type Item = T;
        fn next(&mut self) -> Option<Self::Item> {
            self.inner.lock().ok()?.pop_front()
        }
    }
}
