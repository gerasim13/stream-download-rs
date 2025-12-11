use std::collections::VecDeque;
use std::io::{Read, Result as IoResult, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, trace};

use async_ringbuf::{AsyncHeapCons, AsyncHeapProd, AsyncHeapRb, traits::*};
use bytes::Bytes;
use symphonia::core::audio::GenericAudioBufferRef;
use symphonia::core::codecs::audio::AudioDecoderOptions;
use symphonia::core::formats::probe::Hint;
use symphonia::core::formats::{FormatOptions, TrackType};
use symphonia::core::io::MediaSource;
use symphonia::core::meta::MetadataOptions;
use symphonia::default::{get_codecs, get_probe};

use crate::{AudioProcessor, AudioSpec, PlayerEvent};

/// Packet flowing through ByteRing from Producer to Decoder.
/// Always carries init and media together, enabling re-probe when init_hash changes.
#[derive(Debug, Clone)]
pub struct Packet {
    pub init_hash: u64,
    pub init_bytes: Bytes,
    pub media_bytes: Bytes,
    pub variant_index: Option<usize>,
    pub segment_duration: std::time::Duration,
    pub segment_sequence: u64,
}

/// Blocking feeder implementing Read over a queue of byte chunks.
/// Decoder task pushes init/media chunks here before calling into Symphonia.
#[derive(Debug, Default)]
pub struct Feeder {
    queue: VecDeque<Bytes>,
    pos_in_front: usize,
    eof: bool,
    buffered_len: usize,
}

impl Feeder {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            pos_in_front: 0,
            eof: false,
            buffered_len: 0,
        }
    }

    pub fn push_bytes(&mut self, chunk: Bytes) {
        if !chunk.is_empty() {
            self.buffered_len = self.buffered_len.saturating_add(chunk.len());
            self.queue.push_back(chunk);
        }
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

    #[allow(dead_code)]
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
        guard.buffered_len = guard.buffered_len.saturating_sub(n);
        Ok(n)
    }
}

impl Seek for FeederMediaSource {
    fn seek(&mut self, _pos: SeekFrom) -> IoResult<u64> {
        unimplemented!()
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

/// State tracking for seamless variant switching.
#[derive(Debug, Clone)]
struct PlaybackState {
    /// Total playback time accumulated from decoded segments.
    total_playback_time: Duration,
    /// Current variant index.
    current_variant_index: Option<usize>,
    /// Total number of decoded frames.
    decoded_frames: u64,
    /// Current sample rate.
    sample_rate: u32,
    /// Current channel count.
    channels: u16,
}

/// Transition state for smooth audio transitions between variants.
#[derive(Debug)]
struct TransitionState {
    /// Number of silence samples remaining to insert.
    silence_samples_remaining: usize,
    /// Number of fade-in samples remaining.
    fade_in_samples_remaining: usize,
    /// Total fade-in length in samples.
    fade_in_length_samples: usize,
    /// Whether transition is active.
    active: bool,
    /// Previous variant index.
    previous_variant_index: Option<usize>,
    /// Current variant index.
    current_variant_index: Option<usize>,
    /// Whether we're processing samples from the new variant yet.
    processing_new_variant: bool,
}

/// Simple DC blocking filter to remove DC offset and prevent clicks.
struct DcBlockingFilter {
    /// Previous input sample.
    prev_input: f32,
    /// Previous output sample.
    prev_output: f32,
    /// Filter coefficient (0.995 is typical for 48kHz).
    coefficient: f32,
}

impl DcBlockingFilter {
    /// Create a new DC blocking filter with appropriate coefficient for sample rate.
    fn new(sample_rate: u32) -> Self {
        // Calculate coefficient for ~3Hz cutoff frequency
        // fc = 3Hz, fs = sample_rate
        // R = 1 - (2πfc/fs)
        let fc = 3.0; // Cutoff frequency in Hz
        let fs = sample_rate as f32;
        let coefficient = 1.0 - (2.0 * std::f32::consts::PI * fc / fs);

        Self {
            prev_input: 0.0,
            prev_output: 0.0,
            coefficient: coefficient.max(0.9).min(0.999), // Clamp to reasonable range
        }
    }

    /// Reset filter state (useful when switching variants).
    fn reset(&mut self) {
        self.prev_input = 0.0;
        self.prev_output = 0.0;
    }

    /// Process samples with DC blocking filter.
    fn process(&mut self, samples: &mut [f32]) {
        // Simple first-order high-pass filter: y[n] = x[n] - x[n-1] + R * y[n-1]
        let r = self.coefficient;

        for sample in samples.iter_mut() {
            let input = *sample;
            let output = input - self.prev_input + r * self.prev_output;
            self.prev_input = input;
            self.prev_output = output;
            *sample = output;
        }
    }
}

impl TransitionState {
    fn new(fade_in_duration_ms: u32, sample_rate: u32, channels: u16) -> Self {
        let fade_in_samples = ((fade_in_duration_ms as f32 / 1000.0) * sample_rate as f32) as usize;

        Self {
            silence_samples_remaining: 0,
            fade_in_samples_remaining: 0,
            fade_in_length_samples: fade_in_samples * channels as usize,
            active: false,
            previous_variant_index: None,
            current_variant_index: None,
            processing_new_variant: false,
        }
    }

    /// Process samples, applying fade-in if transition is active.
    /// Returns the number of samples processed.
    fn process(
        &mut self,
        samples: &mut [f32],
        _pcm_prod: &mut AsyncHeapProd<f32>,
        _rt: &tokio::runtime::Runtime,
        need_reopen: bool,
    ) -> usize {
        // If we need to reopen (new decoder), start transition
        if need_reopen && !self.active {
            debug!(
                "TransitionState: starting fade-in for new decoder, fade_in_length_samples={}",
                self.fade_in_length_samples
            );
            // No silence, only fade-in for smoother transition
            self.silence_samples_remaining = 0;
            self.fade_in_samples_remaining = self.fade_in_length_samples;
            self.active = true;
        }

        if !self.active {
            return samples.len();
        }

        let mut samples_processed = 0;
        let total_samples = samples.len();

        debug!(
            "TransitionState: processing transition, active={}, silence_remaining={}, fade_remaining={}, total_samples={}",
            self.active,
            self.silence_samples_remaining,
            self.fade_in_samples_remaining,
            total_samples
        );

        // Apply fade-in to samples
        if self.fade_in_samples_remaining > 0 {
            let samples_remaining = total_samples - samples_processed;
            let fade_to_apply = std::cmp::min(self.fade_in_samples_remaining, samples_remaining);

            for i in 0..fade_to_apply {
                let sample_index = samples_processed + i;
                let fade_progress = 1.0
                    - (self.fade_in_samples_remaining - i) as f32
                        / self.fade_in_length_samples as f32;
                let gain = fade_progress.max(0.0).min(1.0);
                samples[sample_index] *= gain;
            }

            self.fade_in_samples_remaining -= fade_to_apply;
            samples_processed += fade_to_apply;

            // If we still have fade to apply, return
            if self.fade_in_samples_remaining > 0 || samples_processed >= total_samples {
                return samples_processed;
            }
        }

        // Transition complete
        if self.fade_in_samples_remaining == 0 {
            debug!("TransitionState: fade-in complete");
            self.active = false;
            self.processing_new_variant = false;
        }

        // Process any remaining samples normally
        samples_processed + (total_samples - samples_processed)
    }

    /// Update variant tracking and prepare for transition if needed.
    /// Returns true if variant changed.
    fn update_variant(
        &mut self,
        current_variant_index: Option<usize>,
        sample_rate: u32,
        channels: u16,
    ) -> bool {
        let variant_changed = self.previous_variant_index != current_variant_index;

        if variant_changed {
            debug!(
                "TransitionState: variant changed from {:?} to {:?}, sample_rate={}, channels={}",
                self.previous_variant_index, current_variant_index, sample_rate, channels
            );

            self.previous_variant_index = self.current_variant_index;
            self.current_variant_index = current_variant_index;

            // Store fade-in length for transition
            let fade_in_ms = 20;
            self.fade_in_length_samples =
                ((fade_in_ms as f32 / 1000.0) * sample_rate as f32) as usize * channels as usize;

            debug!(
                "TransitionState: fade_in_length_samples={} ({}ms)",
                self.fade_in_length_samples, fade_in_ms
            );

            return true;
        }

        false
    }
}

impl PlaybackState {
    fn new(sample_rate: u32, channels: u16) -> Self {
        Self {
            total_playback_time: Duration::ZERO,
            current_variant_index: None,
            decoded_frames: 0,
            sample_rate,
            channels,
        }
    }

    /// Update playback state with a new segment.
    fn update_with_segment(&mut self, segment_duration: Duration, variant_index: Option<usize>) {
        self.total_playback_time += segment_duration;
        self.current_variant_index = variant_index;
    }

    /// Calculate approximate segment index based on playback time.
    /// Returns (segment_index, time_offset_within_segment).
    fn find_segment_position(&self, segment_duration: Duration) -> (u64, Duration) {
        if segment_duration == Duration::ZERO {
            return (0, Duration::ZERO);
        }

        let total_secs = self.total_playback_time.as_secs_f64();
        let segment_secs = segment_duration.as_secs_f64();
        let segment_index = (total_secs / segment_secs).floor() as u64;
        let time_offset =
            Duration::from_secs_f64(total_secs - (segment_index as f64 * segment_secs));

        (segment_index, time_offset)
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

    /// Optional event callback to bubble up player events without tying to a hub here.
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
            on_event: None,
        }
    }

    /// Helper to probe a new reader from fresh feeder with init+first media bytes.
    fn open_reader(
        feeder_arc: Arc<Mutex<Feeder>>,
    ) -> Result<
        (
            Box<dyn symphonia::core::formats::FormatReader + Send + Sync>,
            String,
        ),
        symphonia::core::errors::Error,
    > {
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
    }

    /// Helper to prebuffer additional packets into feeder to reduce initial underflow/glitches.
    fn prebuffer_packets(
        rt: &tokio::runtime::Runtime,
        byte_cons: &mut AsyncHeapCons<Packet>,
        feeder: &Arc<Mutex<Feeder>>,
    ) {
        let prebuffer_budget: usize = 64 * 1024; // bytes
        let start_prebuffer = std::time::Instant::now();
        loop {
            // Stop if enough bytes buffered.
            if feeder.lock().unwrap().buffered_len >= prebuffer_budget {
                break;
            }
            // Stop if we spent too much time prebuffering.
            if start_prebuffer.elapsed() > std::time::Duration::from_millis(10) {
                break;
            }
            // Try to fetch one more packet with a very short timeout.
            let next_opt = rt.block_on(async {
                tokio::time::timeout(std::time::Duration::from_millis(1), byte_cons.pop()).await
            });
            match next_opt {
                Ok(Some(extra_pkt)) => {
                    if let Ok(mut guard) = feeder.lock() {
                        // Only media bytes; init_hash change will re-open on the next outer loop iteration.
                        guard.push_bytes(extra_pkt.media_bytes.clone());
                    }
                }
                Ok(None) => break, // producer closed
                Err(_) => break,   // timeout
            }
        }
    }

    /// Create audio decoder from format and send format change events.
    fn create_audio_decoder(
        format: Box<dyn symphonia::core::formats::FormatReader + Send + Sync>,
        container_name: &str,
        packet: &Packet,
        current_sample_rate: u32,
        current_channels: u16,
        dec_opts: &AudioDecoderOptions,
        on_event: &Option<Arc<dyn Fn(PlayerEvent) + Send + Sync>>,
    ) -> Result<
        (
            Box<dyn symphonia::core::formats::FormatReader + Send + Sync>,
            Box<dyn symphonia::core::codecs::audio::AudioDecoder + Send + Sync>,
        ),
        String,
    > {
        // Select default audio track and get audio parameters.
        let (_tid, ap) = match format.default_track(TrackType::Audio) {
            Some(t) => match t.codec_params.as_ref().and_then(|cp| cp.audio()).cloned() {
                Some(ap) => (t.id, ap),
                None => {
                    return Err("no audio params on selected track".into());
                }
            },
            None => {
                return Err("no default audio track".into());
            }
        };

        // Create decoder.
        match get_codecs().make_audio_decoder(&ap, dec_opts) {
            Ok(dec) => {
                // Emit events
                if let Some(cb) = on_event {
                    cb(PlayerEvent::VariantSwitched {
                        from: None,
                        to: packet.variant_index.unwrap_or(0),
                        reason: "init switch".into(),
                    });
                    cb(PlayerEvent::FormatChanged {
                        sample_rate: ap.sample_rate.unwrap_or(current_sample_rate),
                        channels: ap
                            .channels
                            .map(|c| c.count() as u16)
                            .unwrap_or(current_channels),
                        codec: None,
                        container: Some(container_name.to_string()),
                    });
                }
                Ok((format, dec))
            }
            Err(e) => Err(format!("decoder make failed: {e}")),
        }
    }

    /// Warmup decoder by decoding a few packets to prebuffer audio data.
    fn warmup_decoder(
        format: &mut Option<Box<dyn symphonia::core::formats::FormatReader + Send + Sync>>,
        decoder: &mut Option<Box<dyn symphonia::core::codecs::audio::AudioDecoder + Send + Sync>>,
        rt: &tokio::runtime::Runtime,
        pcm_prod: &mut AsyncHeapProd<f32>,
    ) {
        let mut warmup_packets = 2usize;
        let mut tmp_warm: Vec<f32> = Vec::new();
        while warmup_packets > 0 {
            if let (Some(f), Some(d)) = (format.as_mut(), decoder.as_mut()) {
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

    /// Decode available packets from format/decoder.
    fn decode_available_packets(
        format: &mut Option<Box<dyn symphonia::core::formats::FormatReader + Send + Sync>>,
        decoder: &mut Option<Box<dyn symphonia::core::codecs::audio::AudioDecoder + Send + Sync>>,
        tmp: &mut Vec<f32>,
        processors: Arc<Mutex<Vec<Arc<dyn AudioProcessor>>>>,
        output_spec: AudioSpec,
        rt: &tokio::runtime::Runtime,
        pcm_prod: &mut AsyncHeapProd<f32>,
        dc_filter: &mut DcBlockingFilter,
        transition_state: &mut TransitionState,
        is_first_decode_after_reopen: &mut bool,
    ) {
        loop {
            // If format/decoder not ready, break to fetch next Packet.
            let (format, decoder) = match (format.as_mut(), decoder.as_mut()) {
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

                            // Apply DC blocking filter to remove DC offset
                            dc_filter.process(&mut tmp[..needed]);

                            // Apply transition for smooth transitions
                            debug!(
                                "decode_available_packets: calling transition_state.process with need_reopen={}, samples={}",
                                *is_first_decode_after_reopen, needed
                            );
                            let processed_samples = transition_state.process(
                                &mut tmp[..needed],
                                pcm_prod,
                                rt,
                                *is_first_decode_after_reopen,
                            );
                            debug!(
                                "decode_available_packets: transition processed {} of {} samples",
                                processed_samples, needed
                            );

                            // Reset the flag after first use
                            *is_first_decode_after_reopen = false;

                            // Only push processed samples (may be less than needed if silence was inserted)
                            let samples_to_push = &tmp[..processed_samples];

                            // Push into PCM ring with backpressure (await space).
                            // We are in blocking thread; block_on push().
                            rt.block_on(async {
                                for &s in samples_to_push {
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

    /// Blocking decoder loop that pulls Packets, (re)probes on init change and pushes PCM.
    pub fn spawn_decoder_loop(
        &mut self,
        output_spec: AudioSpec,
        processors: Arc<Mutex<Vec<Arc<dyn AudioProcessor>>>>,
    ) -> tokio::task::JoinHandle<()> {
        // Move required ring ends and settings into the decoding task.
        let mut byte_cons = self.byte_cons.take().expect("byte_cons already taken");
        let mut pcm_prod = self.pcm_prod.take().expect("pcm_prod already taken");
        let on_event = self.on_event.clone();

        tokio::task::spawn_blocking(move || {
            let mut last_init_hash: Option<u64> = None;
            let mut feeder = Arc::new(Mutex::new(Feeder::new()));
            let mut _container_name: Option<String>;

            let current_sample_rate = output_spec.sample_rate;
            let current_channels = output_spec.channels;

            // State for current format/decoder.
            let mut maybe_format: Option<
                Box<dyn symphonia::core::formats::FormatReader + Send + Sync>,
            > = None;
            let mut maybe_decoder: Option<
                Box<dyn symphonia::core::codecs::audio::AudioDecoder + Send + Sync>,
            > = None;
            let dec_opts = AudioDecoderOptions::default();

            // Temporary interleaved buffer for converted f32 samples.
            let mut tmp: Vec<f32> = Vec::new();

            // Local runtime to block_on async ring operations from this blocking thread.
            let rt = tokio::runtime::Builder::new_current_thread()
                .thread_name("decoder_worker")
                .enable_all()
                .build()
                .expect("tokio rt for consumer");

            // Initialize transition state for smooth transitions (fade-in on switch)
            let mut transition_state = TransitionState::new(
                20, // 20ms fade-in (longer for smoother transition)
                output_spec.sample_rate,
                output_spec.channels,
            );

            // Initialize DC blocking filter to remove DC offset
            let mut dc_filter = DcBlockingFilter::new(output_spec.sample_rate);

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

                // Update variant tracking
                let variant_changed = transition_state.update_variant(
                    packet.variant_index,
                    output_spec.sample_rate,
                    output_spec.channels,
                );

                if variant_changed {
                    // Reset DC filter on variant change to prevent filter state mismatch
                    dc_filter.reset();
                    debug!(
                        "PipelineRunner: variant changed to {:?}, need_reopen={}, init_hash={:?}->{:?}",
                        packet.variant_index, need_reopen, last_init_hash, new_hash
                    );
                }

                let mut is_first_decode_after_reopen = false;

                if need_reopen {
                    debug!(
                        "PipelineRunner: need_reopen=true, last_init_hash={:?}, new_hash={:?}",
                        last_init_hash, new_hash
                    );
                    // New feeder and reader.
                    feeder = Arc::new(Mutex::new(Feeder::new()));
                    {
                        let mut guard = feeder.lock().unwrap();
                        guard.push_bytes(packet.init_bytes.clone());
                        guard.push_bytes(packet.media_bytes.clone());
                    }
                    // Prebuffer additional packets into feeder to reduce initial underflow/glitches.
                    Self::prebuffer_packets(&rt, &mut byte_cons, &feeder);

                    match Self::open_reader(feeder.clone()) {
                        Ok((format, cn)) => {
                            _container_name = Some(cn.clone());
                            // Select default audio track and build decoder.
                            match Self::create_audio_decoder(
                                format,
                                &cn,
                                &packet,
                                current_sample_rate,
                                current_channels,
                                &dec_opts,
                                &on_event,
                            ) {
                                Ok((format, dec)) => {
                                    maybe_decoder = Some(dec);
                                    maybe_format = Some(format);
                                    last_init_hash = Some(new_hash);
                                    // Warmup decode to prebuffer a bit and reduce audible glitches.
                                    Self::warmup_decoder(
                                        &mut maybe_format,
                                        &mut maybe_decoder,
                                        &rt,
                                        &mut pcm_prod,
                                    );
                                    // This is the first decode after reopening
                                    is_first_decode_after_reopen = true;
                                    debug!(
                                        "PipelineRunner: decoder reopened, is_first_decode_after_reopen={}",
                                        is_first_decode_after_reopen
                                    );
                                }
                                Err(e) => {
                                    if let Some(cb) = &on_event {
                                        cb(PlayerEvent::Error { message: e });
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
                Self::decode_available_packets(
                    &mut maybe_format,
                    &mut maybe_decoder,
                    &mut tmp,
                    processors.clone(),
                    output_spec,
                    &rt,
                    &mut pcm_prod,
                    &mut dc_filter,
                    &mut transition_state,
                    &mut is_first_decode_after_reopen,
                );
            }

            // End-of-stream
            if let Some(cb) = &on_event {
                cb(PlayerEvent::EndOfStream);
            }
        })
    }
}
