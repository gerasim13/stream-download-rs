/*!
Pipeline internals

This module defines a minimal but functional skeleton for the future audio pipeline implementation.
It introduces:
- Packet: a self-contained message carrying init+media bytes (and optional metadata).
- FeederMediaSource: a blocking Read/Seek bridge over an internal fifo fed by Packets.
- PipelineRunner: orchestrates producer/decoder using async_ringbuf for bytes and ringbuf for PCM.

Design:
- Producer (Tokio task) writes Packet into an async SPSC ring (ByteRing).
- Decoder (spawn_blocking) reads Packet from ByteRing, re-probes when init_hash changes,
  decodes to f32, resamples, applies processors, and writes PCM into the PcmRing (SPSC).
- On variant switch (init_hash change): flush PcmRing and apply a short fade-out/in.

This file still carries placeholders for submodules (source_hls/http, resample, process),
but now wires the core types needed to evolve the pipeline.
*/

use std::collections::VecDeque;
use std::io::{Read, Result as IoResult, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_ringbuf::{AsyncHeapCons, AsyncHeapProd, AsyncHeapRb, traits::*};
use bytes::Bytes;
use symphonia::core::io::MediaSource;

use stream_download_hls::SelectionMode;
use tracing::{debug, info, warn};

use crate::{AbrConfig, AudioOptions, AudioProcessor, AudioSpec, HlsConfig, PlayerEvent};

/// Backend configuration for the pipeline.
///
/// This enumerates the supported audio sources: HLS (with configs) or HTTP (single resource).
#[derive(Debug, Clone)]
pub enum BackendConfig {
    Hls {
        url: String,
        hls: HlsConfig,
        abr: AbrConfig,
        mode: SelectionMode,
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

/// Packet flowing through ByteRing from Producer to Decoder.
/// Always carries init and media together, enabling re-probe when init_hash changes.
#[derive(Debug, Clone)]
pub struct Packet {
    pub init_hash: u64,
    pub init_bytes: Bytes,
    pub media_bytes: Bytes,
    pub variant_index: Option<usize>,
    pub codec_tag: Option<String>,
}

/// Blocking feeder implementing Read over a queue of byte chunks.
/// Decoder task pushes init/media chunks here before calling into Symphonia.
#[derive(Debug, Default)]
pub struct Feeder {
    queue: VecDeque<Bytes>,
    pos_in_front: usize,
    eof: bool,
}

impl Feeder {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            pos_in_front: 0,
            eof: false,
        }
    }

    pub fn push_bytes(&mut self, chunk: Bytes) {
        if !chunk.is_empty() {
            self.queue.push_back(chunk);
        }
    }

    pub fn set_eof(&mut self) {
        self.eof = true;
    }

    fn pop_front_if_empty(&mut self) {
        if let Some(front) = self.queue.front() {
            if self.pos_in_front >= front.len() {
                self.queue.pop_front();
                self.pos_in_front = 0;
            }
        }
    }
}

/// A minimal blocking MediaSource over Feeder
pub struct FeederMediaSource {
    inner: Arc<Mutex<Feeder>>,
    // optional: gating seek until probe is complete
    seek_enabled: bool,
}

impl FeederMediaSource {
    pub fn new(inner: Arc<Mutex<Feeder>>) -> Self {
        Self {
            inner,
            seek_enabled: false,
        }
    }

    pub fn enable_seek(&mut self) {
        self.seek_enabled = true;
    }
}

impl Read for FeederMediaSource {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let mut guard = self.inner.lock().unwrap();

        loop {
            guard.pop_front_if_empty();
            if guard.queue.is_empty() {
                if guard.eof {
                    return Ok(0);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "no data available",
                ));
            } else {
                break;
            }
        }

        let front = guard.queue.front().unwrap();
        let start = guard.pos_in_front;
        let n = std::cmp::min(buf.len(), front.len() - start);
        buf[..n].copy_from_slice(&front.slice(start..start + n));
        guard.pos_in_front += n;
        Ok(n)
    }
}

impl Seek for FeederMediaSource {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        if !self.seek_enabled {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "seek disabled",
            ));
        }
        match pos {
            SeekFrom::Start(0) => {
                let mut guard = self.inner.lock().unwrap();
                guard.queue.clear();
                guard.pos_in_front = 0;
                Ok(0)
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "seek not supported",
            )),
        }
    }
}

impl MediaSource for FeederMediaSource {
    fn is_seekable(&self) -> bool {
        self.seek_enabled
    }

    fn byte_len(&self) -> Option<u64> {
        None
    }
}
/// Runner wiring async producer and blocking decoder.
///
/// Note: This is a skeleton; actual decode/probe/resample is implemented elsewhere.
pub struct PipelineRunner {
    // Async byte ring: producer -> decoder
    pub byte_prod: Option<AsyncHeapProd<Packet>>,
    pub byte_cons: Option<AsyncHeapCons<Packet>>,

    // PCM ring: decoder -> consumer (async)
    pub pcm_prod: Option<AsyncHeapProd<f32>>,
    pub pcm_cons: Option<AsyncHeapCons<f32>>,

    // Fade configuration (ms)
    pub fade_ms: u32,

    // Optional event callback to bubble up player events without tying to a hub here.
    pub on_event: Option<Arc<dyn Fn(PlayerEvent) + Send + Sync>>,
}

impl PipelineRunner {
    pub fn new(byte_capacity: usize, pcm_capacity: usize) -> Self {
        let byte_rb = AsyncHeapRb::<Packet>::new(byte_capacity);
        let (byte_prod, byte_cons) = byte_rb.split();

        let pcm_rb = AsyncHeapRb::<f32>::new(pcm_capacity);
        let (pcm_prod, pcm_cons) = pcm_rb.split();

        Self {
            byte_prod: Some(byte_prod),
            byte_cons: Some(byte_cons),
            pcm_prod: Some(pcm_prod),
            pcm_cons: Some(pcm_cons),
            fade_ms: 5,
            on_event: None,
        }
    }

    /// Apply a short linear fade-out by appending a ramp-down to the PCM ring.
    /// Note: Does not drain the consumer side; consumer should read remaining samples.
    pub async fn fade_out_and_flush(
        pcm_prod: &mut AsyncHeapProd<f32>,
        sample_rate: u32,
        channels: u16,
        fade_ms: u32,
    ) {
        let frames = (fade_ms as u64 * sample_rate as u64 / 1000) as usize;
        if frames == 0 || channels == 0 {
            return;
        }
        let samples = frames.saturating_mul(channels as usize);
        for i in 0..samples {
            // linear ramp down from 1.0 -> 0.0 across 'samples'
            let t = 1.0 - (i as f32 / samples as f32);
            let _ = pcm_prod.push(0.0_f32 * t).await;
        }
    }

    /// Apply a short linear fade-in in place to a just-produced buffer slice.
    pub fn apply_fade_in(buf: &mut [f32], channels: u16, fade_ms: u32) {
        if fade_ms == 0 || channels == 0 {
            return;
        }
        // Apply a short linear fade-in on the beginning of 'buf'.
        // We assume 'buf' contains interleaved samples. Use a small fixed-time fade.
        let frames = (fade_ms as usize) * 48; // ~5ms @ 48kHz -> 240 frames for fade_ms=5
        let max_samples = frames.saturating_mul(channels as usize).min(buf.len());
        if max_samples == 0 {
            return;
        }
        for i in 0..max_samples {
            let t = i as f32 / max_samples as f32; // 0..1
            buf[i] *= t;
        }
    }

    /// Blocking decoder loop that pulls Packets, (re)probes on init change and pushes PCM.
    pub fn spawn_decoder_loop(
        &mut self,
        output_spec: AudioSpec,
        _events: EventHub,
        processors: Arc<Mutex<Vec<Arc<dyn AudioProcessor>>>>,
    ) -> tokio::task::JoinHandle<()> {
        use symphonia::core::audio::GenericAudioBufferRef;
        use symphonia::core::codecs::audio::AudioDecoderOptions;
        use symphonia::core::formats::probe::Hint;
        use symphonia::core::formats::{FormatOptions, TrackType};
        use symphonia::core::io::MediaSource;
        use symphonia::core::meta::MetadataOptions;
        use symphonia::default::{get_codecs, get_probe};

        // Move required ring ends and settings into the decoding task.
        let mut byte_cons = self.byte_cons.take().expect("byte_cons already taken");
        let mut pcm_prod = self.pcm_prod.take().expect("pcm_prod already taken");
        let fade_ms = self.fade_ms;
        let on_event = self.on_event.clone();

        tokio::task::spawn_blocking(move || {
            let mut last_init_hash: Option<u64> = None;
            let mut feeder = Arc::new(Mutex::new(Feeder::new()));
            let mut container_name: Option<String> = None;

            let mut current_sample_rate = output_spec.sample_rate;
            let mut current_channels = output_spec.channels;

            // Track last seen variant to emit from->to in VariantSwitched
            let mut last_variant_index: Option<usize> = None;

            // Helper to probe a new reader from fresh feeder with init+first media bytes.
            let mut open_reader = |feeder_arc: Arc<Mutex<Feeder>>| {
                let mss = symphonia::core::io::MediaSourceStream::new(
                    Box::new(FeederMediaSource::new(feeder_arc)) as Box<dyn MediaSource>,
                    symphonia::core::io::MediaSourceStreamOptions::default(),
                );
                let hint = Hint::new();
                match get_probe().probe(
                    &hint,
                    mss,
                    FormatOptions::default(),
                    MetadataOptions::default(),
                ) {
                    Ok(probed) => {
                        let cn = probed.format_info().short_name.to_string();
                        Ok((probed, cn))
                    }
                    Err(e) => Err(e),
                }
            };

            // State for current format/decoder.
            let mut maybe_format: Option<
                Box<dyn symphonia::core::formats::FormatReader + Send + Sync>,
            > = None;
            let mut maybe_decoder: Option<_> = None;
            let dec_opts = AudioDecoderOptions::default();

            // Temporary interleaved buffer for converted f32 samples.
            let mut tmp: Vec<f32> = Vec::new();

            // Local runtime to block_on async ring operations from this blocking thread.
            let rt = tokio::runtime::Builder::new_current_thread()
                .thread_name("decoder_worker")
                .enable_all()
                .build()
                .expect("tokio rt for consumer");

            loop {
                // Pull next packet (async). We are in blocking thread, so block_on.
                let packet_opt = rt.block_on(byte_cons.pop());
                let packet = match packet_opt {
                    Some(pkt) => pkt,
                    None => {
                        // Producer closed.
                        break;
                    }
                };

                let new_hash = packet.init_hash;
                let need_reopen = last_init_hash.map(|h| h != new_hash).unwrap_or(true);

                if need_reopen {
                    // Optional: fade-out before format switch.
                    rt.block_on(PipelineRunner::fade_out_and_flush(
                        &mut pcm_prod,
                        current_sample_rate,
                        current_channels,
                        fade_ms,
                    ));

                    // New feeder and reader.
                    feeder = Arc::new(Mutex::new(Feeder::new()));
                    {
                        let mut guard = feeder.lock().unwrap();
                        guard.push_bytes(packet.init_bytes.clone());
                        guard.push_bytes(packet.media_bytes.clone());
                    }

                    match open_reader(feeder.clone()) {
                        Ok((mut format, cn)) => {
                            container_name = Some(cn.clone());
                            // Select default audio track and build decoder.
                            let (_tid, ap) = match format.default_track(TrackType::Audio) {
                                Some(t) => {
                                    match t.codec_params.as_ref().and_then(|cp| cp.audio()).cloned()
                                    {
                                        Some(ap) => (t.id, ap),
                                        None => {
                                            if let Some(cb) = &on_event {
                                                cb(PlayerEvent::Error {
                                                    message: "no audio params on selected track"
                                                        .into(),
                                                });
                                            }
                                            return;
                                        }
                                    }
                                }
                                None => {
                                    if let Some(cb) = &on_event {
                                        cb(PlayerEvent::Error {
                                            message: "no default audio track".into(),
                                        });
                                    }
                                    return;
                                }
                            };
                            match get_codecs().make_audio_decoder(&ap, &dec_opts) {
                                Ok(dec) => {
                                    maybe_decoder = Some(dec);
                                    maybe_format = Some(format);
                                    last_init_hash = Some(new_hash);
                                    // Emit events
                                    if let Some(cb) = &on_event {
                                        let cont = container_name.clone();
                                        cb(PlayerEvent::VariantSwitched {
                                            from: None,
                                            to: packet.variant_index.unwrap_or(0),
                                            reason: "init switch".into(),
                                        });
                                        cb(PlayerEvent::FormatChanged {
                                            sample_rate: ap
                                                .sample_rate
                                                .unwrap_or(current_sample_rate),
                                            channels: ap
                                                .channels
                                                .map(|c| c.count() as u16)
                                                .unwrap_or(current_channels),
                                            codec: None,
                                            container: cont,
                                        });
                                    }
                                    // Warmup decode to prebuffer a bit and reduce audible glitches.
                                    let mut warmup_packets = 2usize;
                                    let mut tmp_warm: Vec<f32> = Vec::new();
                                    while warmup_packets > 0 {
                                        if let (Some(f), Some(d)) =
                                            (maybe_format.as_mut(), maybe_decoder.as_mut())
                                        {
                                            match f.next_packet() {
                                                Ok(Some(pkt)) => match d.decode(&pkt) {
                                                    Ok(decoded) => {
                                                        let gab: GenericAudioBufferRef = decoded;
                                                        let chans = gab.num_planes();
                                                        let frames = gab.frames();
                                                        let needed = chans * frames;
                                                        if needed > 0 {
                                                            let start = tmp_warm.len();
                                                            tmp_warm.resize(start + needed, 0.0);
                                                            gab.copy_to_slice_interleaved::<f32, _>(
                                                                    &mut tmp_warm[start..start + needed],
                                                                );
                                                        }
                                                        warmup_packets -= 1;
                                                    }
                                                    Err(_) => break,
                                                },
                                                _ => break,
                                            }
                                        } else {
                                            break;
                                        }
                                    }
                                    rt.block_on(async {
                                        for &s in &tmp_warm {
                                            if pcm_prod.push(s).await.is_err() {
                                                break;
                                            }
                                        }
                                    });
                                }
                                Err(e) => {
                                    if let Some(cb) = &on_event {
                                        cb(PlayerEvent::Error {
                                            message: format!("decoder make failed: {e}"),
                                        });
                                    }
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            if let Some(cb) = &on_event {
                                cb(PlayerEvent::Error {
                                    message: format!("probe failed: {e}"),
                                });
                            }
                            return;
                        }
                    }
                } else {
                    // Same init: just append media bytes into feeder.
                    if let Ok(mut guard) = feeder.lock() {
                        guard.push_bytes(packet.media_bytes.clone());
                    }
                }

                // Decode available packets until format needs more bytes or EOF.
                loop {
                    // If format/decoder not ready, break to fetch next Packet.
                    let (format, decoder) = match (maybe_format.as_mut(), maybe_decoder.as_mut()) {
                        (Some(f), Some(d)) => (f, d),
                        _ => break,
                    };

                    match format.next_packet() {
                        Ok(Some(pkt)) => {
                            match decoder.decode(&pkt) {
                                Ok(decoded) => {
                                    let gab: GenericAudioBufferRef = decoded;
                                    let chans = gab.num_planes();
                                    let frames = gab.frames();
                                    let needed = chans * frames;
                                    if needed == 0 {
                                        continue;
                                    }

                                    // Resize temp buffer and convert to interleaved f32.
                                    tmp.resize(needed, 0.0);
                                    gab.copy_to_slice_interleaved::<f32, _>(&mut tmp[..needed]);

                                    // TODO: resample if input != output_spec (pass-through for now).

                                    // Apply processors chain (in-place).
                                    if let Ok(procs) = processors.lock() {
                                        for p in procs.iter() {
                                            let _ = p.process(&mut tmp[..needed], output_spec);
                                        }
                                    }

                                    // Push into PCM ring with backpressure (await space).
                                    // We are in blocking thread; block_on push().
                                    rt.block_on(async {
                                        for &s in &tmp[..needed] {
                                            if pcm_prod.push(s).await.is_err() {
                                                break;
                                            }
                                        }
                                    });
                                }
                                Err(_) => {
                                    // Decoder wants more bytes or encountered recoverable error; break to fetch next Packet.
                                    break;
                                }
                            }
                        }
                        Ok(None) => {
                            // End of current feeder stream; wait for more packets.
                            break;
                        }
                        Err(e) => {
                            // If the underlying error is WouldBlock (no data yet), break to fetch more bytes.
                            let is_would_block = std::error::Error::source(&e)
                                .and_then(|src| src.downcast_ref::<std::io::Error>())
                                .map(|ioe| ioe.kind() == std::io::ErrorKind::WouldBlock)
                                .unwrap_or(false);
                            if is_would_block {
                                break;
                            }
                            // Other non-fatal errors: also break to fetch more.
                            break;
                        }
                    }
                }
            }

            // End-of-stream
            if let Some(cb) = &on_event {
                cb(PlayerEvent::EndOfStream);
            }
        })
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
