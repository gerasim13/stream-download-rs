use std::collections::VecDeque;
use std::io::{Read, Result as IoResult, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use tracing::debug;

use bytes::Bytes;
use kanal::{AsyncReceiver, AsyncSender, ReceiveError};
use ringbuf::{
    HeapCons, HeapProd, HeapRb,
    traits::{Consumer, Producer, Split},
};
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

/// Pipeline containing the decoding logic
pub struct Pipeline {
    /// Current feeder for media data
    feeder: Arc<Mutex<Feeder>>,
    /// Current format reader
    format: Option<Box<dyn symphonia::core::formats::FormatReader + Send + Sync>>,
    /// Current audio decoder
    decoder: Option<Box<dyn symphonia::core::codecs::audio::AudioDecoder + Send + Sync>>,
    /// Container name of current stream
    container_name: Option<String>,
    /// Last init hash to detect changes
    last_init_hash: Option<u64>,
    /// Temporary buffer for converted samples
    tmp_buffer: Vec<f32>,
    /// Flag indicating first decode after reopening
    is_first_decode_after_reopen: bool,
    /// Optional: gating seek until probe is complete
    seek_enabled: bool,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            feeder: Arc::new(Mutex::new(Feeder::new())),
            format: None,
            decoder: None,
            container_name: None,
            last_init_hash: None,
            tmp_buffer: Vec::new(),
            is_first_decode_after_reopen: false,
            seek_enabled: false,
        }
    }

    /// Create a MediaSource from this pipeline's feeder
    pub fn as_media_source(&self) -> Box<dyn MediaSource> {
        Box::new(Pipeline {
            feeder: self.feeder.clone(),
            format: None,
            decoder: None,
            container_name: None,
            last_init_hash: None,
            tmp_buffer: Vec::new(),
            is_first_decode_after_reopen: false,
            seek_enabled: false,
        })
    }

    /// Helper to probe a new reader from current feeder.
    pub fn open_reader(
        &self,
    ) -> Result<
        (
            Box<dyn symphonia::core::formats::FormatReader + Send + Sync>,
            String,
        ),
        symphonia::core::errors::Error,
    > {
        let mss = symphonia::core::io::MediaSourceStream::new(
            self.as_media_source(),
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

    /// Create audio decoder from format and send format change events.
    pub fn create_audio_decoder(
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

    /// Process a packet: handle init hash changes and feed data to decoder
    pub fn process_packet(
        &mut self,
        packet: Packet,
        on_event: &Option<Arc<dyn Fn(PlayerEvent) + Send + Sync>>,
        output_spec: AudioSpec,
    ) -> Result<(), String> {
        let new_hash = packet.init_hash;
        let need_reopen = self.last_init_hash.map(|h| h != new_hash).unwrap_or(true);

        if need_reopen {
            debug!(
                "Pipeline: need_reopen=true, last_init_hash={:?}, new_hash={:?}",
                self.last_init_hash, new_hash
            );

            // New feeder and reader.
            self.feeder = Arc::new(Mutex::new(Feeder::new()));
            {
                let mut guard = self.feeder.lock().unwrap();
                guard.push_bytes(packet.init_bytes.clone());
                guard.push_bytes(packet.media_bytes.clone());
            }

            // Reset pipeline state before opening new reader
            self.format = None;
            self.decoder = None;
            self.container_name = None;
            self.last_init_hash = None;
            self.is_first_decode_after_reopen = false;
            self.seek_enabled = false;

            match self.open_reader() {
                Ok((format, cn)) => {
                    self.container_name = Some(cn.clone());

                    // Select default audio track and build decoder.
                    match Self::create_audio_decoder(
                        format,
                        &cn,
                        &packet,
                        output_spec.sample_rate,
                        output_spec.channels,
                        &AudioDecoderOptions::default(),
                        on_event,
                    ) {
                        Ok((format, dec)) => {
                            self.decoder = Some(dec);
                            self.format = Some(format);
                            self.last_init_hash = Some(new_hash);
                            self.is_first_decode_after_reopen = true;

                            debug!(
                                "Pipeline: decoder reopened, is_first_decode_after_reopen={}",
                                self.is_first_decode_after_reopen
                            );
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    return Err(format!("probe failed: {e}"));
                }
            }
        } else {
            // Same init: just append media bytes into feeder.
            if let Ok(mut guard) = self.feeder.lock() {
                guard.push_bytes(packet.media_bytes.clone());
            }
        }

        Ok(())
    }

    /// Decode available packets from format/decoder.
    pub fn decode_available_packets(
        &mut self,
        processors: Arc<Mutex<Vec<Arc<dyn AudioProcessor>>>>,
        output_spec: AudioSpec,
        pcm_prod: &mut HeapProd<f32>,
    ) {
        loop {
            // If format/decoder not ready, break to fetch next Packet.
            let (format, decoder) = match (self.format.as_mut(), self.decoder.as_mut()) {
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
                            self.tmp_buffer.resize(needed, 0.0);
                            gab.copy_to_slice_interleaved::<f32, _>(&mut self.tmp_buffer[..needed]);

                            // TODO: resample if input != output_spec (pass-through for now).

                            // Apply processors chain (in-place).
                            if let Ok(procs) = processors.lock() {
                                for p in procs.iter() {
                                    let _ = p.process(&mut self.tmp_buffer[..needed], output_spec);
                                }
                            }

                            // Reset the flag after first use
                            self.is_first_decode_after_reopen = false;

                            // Only push processed samples (may be less than needed if silence was inserted)
                            let samples_to_push = &self.tmp_buffer[..needed];

                            // Push into PCM ring with backpressure (non-blocking).
                            for &s in samples_to_push {
                                if pcm_prod.try_push(s).is_err() {
                                    break;
                                }
                            }
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
}

impl Read for Pipeline {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let mut guard = self.feeder.lock().unwrap();

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

impl Seek for Pipeline {
    fn seek(&mut self, _pos: SeekFrom) -> IoResult<u64> {
        unimplemented!()
    }
}

impl MediaSource for Pipeline {
    fn is_seekable(&self) -> bool {
        self.seek_enabled
    }

    fn byte_len(&self) -> Option<u64> {
        None
    }
}

/// Runner wiring async producer and blocking decoder.
/// Orchestrates the pipeline and contains the main decoding loop.
pub struct PipelineRunner {
    // Async byte channel: producer -> decoder
    pub byte_tx: Option<AsyncSender<Packet>>,
    pub byte_rx: Option<AsyncReceiver<Packet>>,

    // PCM ring: decoder -> consumer (sync)
    pub pcm_prod: Option<HeapProd<f32>>,
    pub pcm_cons: Option<HeapCons<f32>>,

    /// Optional event callback to bubble up player events without tying to a hub here.
    pub on_event: Option<Arc<dyn Fn(PlayerEvent) + Send + Sync>>,
}

impl PipelineRunner {
    pub fn new(byte_capacity: usize, pcm_capacity: usize) -> Self {
        let (byte_tx, byte_rx) = kanal::bounded_async(byte_capacity);

        let pcm_rb = HeapRb::<f32>::new(pcm_capacity);
        let (pcm_prod, pcm_cons) = pcm_rb.split();

        Self {
            byte_tx: Some(byte_tx),
            byte_rx: Some(byte_rx),
            pcm_prod: Some(pcm_prod),
            pcm_cons: Some(pcm_cons),
            on_event: None,
        }
    }

    /// Pop up to `out.len()` samples from the PCM ring buffer.
    /// Returns the number of samples actually popped.
    pub fn pop_chunk(&mut self, out: &mut [f32]) -> usize {
        let mut n = 0usize;
        while n < out.len() {
            match self
                .pcm_cons
                .as_mut()
                .expect("pcm_consumer already taken")
                .try_pop()
            {
                Some(s) => {
                    out[n] = s;
                    n += 1;
                }
                None => break,
            }
        }
        n
    }

    /// Blocking decoder loop that pulls Packets, orchestrates the pipeline and pushes PCM.
    pub fn spawn_decoder_loop(
        &mut self,
        output_spec: AudioSpec,
        processors: Arc<Mutex<Vec<Arc<dyn AudioProcessor>>>>,
    ) -> tokio::task::JoinHandle<()> {
        // Move required ring ends and settings into the decoding task.
        let byte_rx = self.byte_rx.take().expect("byte_rx already taken");
        let mut pcm_prod = self.pcm_prod.take().expect("pcm_prod already taken");
        let on_event = self.on_event.clone();

        // Clone the async receiver to get a sync receiver for blocking thread
        let sync_byte_rx = byte_rx.clone_sync();

        // Create pipeline instance
        let mut pipeline = Pipeline::new();

        tokio::task::spawn_blocking(move || {
            tracing::info!("Decoder loop started");
            let mut packet_count = 0;
            loop {
                // Pull next packet using sync receiver (blocking).
                let packet = match sync_byte_rx.recv() {
                    Ok(pkt) => {
                        tracing::debug!("Decoder loop: received packet (init_hash={}, init_bytes={}, media_bytes={})",
                            pkt.init_hash, pkt.init_bytes.len(), pkt.media_bytes.len());
                        pkt
                    },
                    Err(ReceiveError::Closed) => {
                        tracing::info!("Decoder loop: producer closed (ReceiveError::Closed)");
                        break;
                    }
                    Err(ReceiveError::SendClosed) => {
                        tracing::info!("Decoder loop: sender closed (ReceiveError::SendClosed)");
                        break;
                    }
                };

                packet_count += 1;
                if packet_count % 10 == 0 {
                    tracing::debug!("Decoder loop: processed {} packets", packet_count);
                }

                // Process the packet through pipeline
                if let Err(e) = pipeline.process_packet(packet, &on_event, output_spec) {
                    tracing::error!("Decoder loop: process_packet failed: {}", e);
                    if let Some(cb) = &on_event {
                        cb(PlayerEvent::Error { message: e.clone() });
                    }
                    tracing::info!("Decoder loop: exiting due to error: {}", e);
                    return;
                }

                // Decode available packets until format needs more bytes or EOF.
                pipeline.decode_available_packets(processors.clone(), output_spec, &mut pcm_prod);
            }

            // End-of-stream
            if let Some(cb) = &on_event {
                cb(PlayerEvent::EndOfStream);
            }
        })
    }
}
