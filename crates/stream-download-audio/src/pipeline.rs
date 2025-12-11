use std::collections::VecDeque;
use std::io::{Read, Result as IoResult, Seek, SeekFrom};
use std::sync::{Arc, Mutex};

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
        processors: &Arc<Mutex<Vec<Arc<dyn AudioProcessor>>>>,
        output_spec: AudioSpec,
        rt: &tokio::runtime::Runtime,
        pcm_prod: &mut AsyncHeapProd<f32>,
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
                    &processors,
                    output_spec,
                    &rt,
                    &mut pcm_prod,
                );
            }

            // End-of-stream
            if let Some(cb) = &on_event {
                cb(PlayerEvent::EndOfStream);
            }
        })
    }
}
