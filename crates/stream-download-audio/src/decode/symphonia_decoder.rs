//! Symphonia-based decoder thread (Symphonia v0.6 / dev-0.6 aligned).
//!
//! This module provides the blocking decoder side of the `stream-download-audio` architecture.
//!
//! ## Why this exists
//! - HTTP/HLS sources are async and yield ordered `SourceMsg` (bytes + boundaries).
//! - Symphonia expects a blocking `Read + Seek` (`MediaSource`) to probe and decode.
//! - HLS may switch variants mid-track; the new variant may use a different codec/container.
//!   In that case we MUST create a new decoder and continue decoding seamlessly.
//!
//! ## Epoch model
//! The async orchestration layer converts ordered init boundaries into **epochs**:
//! - Initial epoch starts immediately (HTTP) or before first init (HLS).
//! - On `HlsInitStart` we close the current epoch and start a new one.
//! - The decoder thread receives `EpochMsg::StartEpoch` with a fresh epoch source.
//!
//! This guarantees bytes from different init epochs never mix in Symphonia input.
//!
//! ## Output contract
//! The decoder thread emits ordered `AudioMsg` to a bounded `tokio::mpsc::Sender<AudioMsg>`:
//! - `AudioControl::DecoderInitialized { reason }` for each epoch
//! - `AudioControl::FormatChanged { spec }` after opening the decoder
//! - `AudioMsg::Pcm(PcmChunk)` batches
//! - a final `AudioControl::EndOfStream` when the thread terminates
//!
//! Resampling/channel mixing is intentionally not implemented yet.

use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::sync::mpsc as std_mpsc;

use tokio::sync::mpsc;

use symphonia::core::audio::GenericAudioBufferRef;
use symphonia::core::codecs::audio::AudioDecoderOptions;
use symphonia::core::formats::probe::Hint;
use symphonia::core::formats::{FormatOptions, FormatReader, TrackType};
use symphonia::core::io::{MediaSource, MediaSourceStream, MediaSourceStreamOptions};
use symphonia::core::meta::MetadataOptions;
use symphonia::default::{get_codecs, get_probe};

use crate::decode::byte_queue::ByteQueueReader;
use crate::types::{AudioControl, AudioMsg, AudioSpec, DecoderLifecycleReason, PcmChunk};

/// Per-epoch compressed bytes input.
///
/// The async side should send compressed `Bytes` into a bounded `tokio::mpsc::Sender`.
/// The decoder thread reads them via a blocking `Read` adapter.
pub type EpochBytesRx = mpsc::Receiver<bytes::Bytes>;

/// Messages sent from the async orchestrator to the blocking decoder thread.
pub enum EpochMsg {
    /// Start a new decode epoch with a fresh bytestream source.
    StartEpoch {
        /// Seekable `MediaSource` for this epoch.
        ///
        /// Note: even for streaming sources, Symphonia requires `Seek`. We provide a minimal,
        /// non-seekable implementation that supports only `SeekFrom::Current(0)`.
        source: Box<dyn MediaSource + Send + Sync>,
        /// Why we (re)initialized.
        reason: DecoderLifecycleReason,
        /// Desired output spec (informational for now).
        output_spec: AudioSpec,
        /// Preferred PCM chunk size in **sample-frames** (not samples).
        pcm_chunk_frames: usize,
    },

    /// Start a new decode epoch backed by an async byte channel.
    ///
    /// This allows the async pipeline to enforce backpressure via bounded `mpsc::Sender`,
    /// avoiding any blocking writes in async contexts.
    StartEpochFromAsyncBytes {
        /// Receiver side of the bounded async byte channel for this epoch.
        ///
        /// The decoder thread will block on this receiver when it needs more bytes.
        bytes_rx: EpochBytesRx,
        /// Why we (re)initialized.
        reason: DecoderLifecycleReason,
        /// Desired output spec (informational for now).
        output_spec: AudioSpec,
        /// Preferred PCM chunk size in **sample-frames** (not samples).
        pcm_chunk_frames: usize,
    },

    /// No more epochs will arrive.
    EndOfStream,
}

/// Spawns the blocking decoder thread.
///
/// - `epoch_rx`: receives epoch boundaries and sources.
/// - `msg_tx`: bounded output channel; provides backpressure.
///
/// This task is designed to run for the lifetime of `AudioDecodeStream`.
pub fn spawn_decoder_thread(
    epoch_rx: std_mpsc::Receiver<EpochMsg>,
    msg_tx: mpsc::Sender<AudioMsg>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let mut runner = DecoderRunner { msg_tx };

        while let Ok(msg) = epoch_rx.recv() {
            match msg {
                EpochMsg::StartEpoch {
                    source,
                    reason,
                    output_spec,
                    pcm_chunk_frames,
                } => {
                    if let Err(e) =
                        runner.decode_epoch(source, reason, output_spec, pcm_chunk_frames)
                    {
                        tracing::error!("decoder epoch failed: {}", e);
                        break;
                    }
                }
                EpochMsg::StartEpochFromAsyncBytes {
                    bytes_rx,
                    reason,
                    output_spec,
                    pcm_chunk_frames,
                } => {
                    let source = epoch_media_source_from_async_bytes(bytes_rx);
                    if let Err(e) =
                        runner.decode_epoch(source, reason, output_spec, pcm_chunk_frames)
                    {
                        tracing::error!("decoder epoch failed: {}", e);
                        break;
                    }
                }
                EpochMsg::EndOfStream => break,
            }
        }

        // Ordered termination for consumers.
        let _ = runner
            .msg_tx
            .blocking_send(AudioMsg::Control(AudioControl::EndOfStream));
        drop(runner.msg_tx);
    })
}

struct DecoderRunner {
    msg_tx: mpsc::Sender<AudioMsg>,
}

impl DecoderRunner {
    fn decode_epoch(
        &mut self,
        source: Box<dyn MediaSource + Send + Sync>,
        reason: DecoderLifecycleReason,
        requested_output_spec: AudioSpec,
        pcm_chunk_frames: usize,
    ) -> io::Result<()> {
        let pcm_chunk_frames = pcm_chunk_frames.max(1);

        // 1) Probe the container/format.
        let (mut format, container_name) = probe_format(source)?;

        // 2) Select default audio track.
        let track = format
            .default_track(TrackType::Audio)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no default audio track"))?;

        let codec_params = track
            .codec_params
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing codec params"))?;

        let audio_params = codec_params
            .audio()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing audio params"))?;

        // 3) Create decoder (codec may differ across epochs).
        let mut decoder = get_codecs()
            .make_audio_decoder(audio_params, &AudioDecoderOptions::default())
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("make_audio_decoder: {e}"),
                )
            })?;

        // 4) Emit ordered lifecycle controls.
        let _ = self
            .msg_tx
            .blocking_send(AudioMsg::Control(AudioControl::DecoderInitialized {
                reason,
            }));

        let spec = AudioSpec {
            sample_rate: audio_params
                .sample_rate
                .unwrap_or(requested_output_spec.sample_rate),
            channels: audio_params
                .channels
                .clone()
                .map(|c| c.count() as u16)
                .unwrap_or(requested_output_spec.channels),
        };

        let _ = self
            .msg_tx
            .blocking_send(AudioMsg::Control(AudioControl::FormatChanged { spec }));

        tracing::info!(
            "decoder epoch opened: container={} sample_rate={} channels={}",
            container_name,
            spec.sample_rate,
            spec.channels
        );

        // 5) Decode packets, emit PCM chunks.
        decode_packets_to_pcm_chunks(
            format.as_mut(),
            decoder.as_mut(),
            spec,
            pcm_chunk_frames,
            &self.msg_tx,
        )
    }
}

/// Probe the container/format using Symphonia v0.6 probe API.
///
/// IMPORTANT:
/// We intentionally do **not** require `Send + Sync` on the returned `FormatReader`.
/// The entire decode pipeline for a given epoch stays on the same blocking thread, so we don't
/// need to move the reader across threads.
///
/// This removes the need for any `unsafe` casting.
fn probe_format(
    source: Box<dyn MediaSource + Send + Sync>,
) -> io::Result<(Box<dyn FormatReader>, String)> {
    let mss = MediaSourceStream::new(source, MediaSourceStreamOptions::default());
    let hint = Hint::new();

    let format: Box<dyn FormatReader> = get_probe()
        .probe(
            &hint,
            mss,
            FormatOptions::default(),
            MetadataOptions::default(),
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("probe: {e}")))?;

    let name = format.format_info().short_name.to_string();

    Ok((format, name))
}

fn decode_packets_to_pcm_chunks(
    format: &mut dyn FormatReader,
    decoder: &mut dyn symphonia::core::codecs::audio::AudioDecoder,
    spec: AudioSpec,
    pcm_chunk_frames: usize,
    msg_tx: &mpsc::Sender<AudioMsg>,
) -> io::Result<()> {
    let channels = spec.channels.max(1) as usize;
    let chunk_samples_target = pcm_chunk_frames.saturating_mul(channels).max(1);

    let mut accum: Vec<f32> = Vec::with_capacity(chunk_samples_target);

    loop {
        let packet_opt = match format.next_packet() {
            Ok(p) => p,
            Err(e) => {
                // IMPORTANT:
                // When switching variants, we close the current byte-epoch to force decoder reinit.
                // Depending on timing and how the container reader buffers, Symphonia may surface
                // EOF-ish errors here (e.g. "unexpected end of file") while it is draining.
                //
                // Treat these as a normal end-of-epoch so the outer loop can proceed to the next
                // epoch (new init / new codec) instead of aborting the whole decoder thread.
                if let symphonia::core::errors::Error::IoError(ioe) = &e {
                    if ioe.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    }
                }
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("next_packet: {e}"),
                ));
            }
        };

        let Some(packet) = packet_opt else {
            break; // EOF for this epoch
        };

        // Decode packet.
        let decoded: GenericAudioBufferRef<'_> = match decoder.decode(&packet) {
            Ok(buf) => buf,
            Err(e) => {
                // Many decode errors are recoverable; continue.
                tracing::debug!("decode error (skipping packet): {}", e);
                continue;
            }
        };

        push_generic_audio_as_f32_interleaved(decoded, channels, &mut accum);

        while accum.len() >= chunk_samples_target {
            let pcm = accum.drain(..chunk_samples_target).collect::<Vec<f32>>();
            let _ = msg_tx.blocking_send(AudioMsg::Pcm(PcmChunk { pcm, spec }));
        }
    }

    if !accum.is_empty() {
        let _ = msg_tx.blocking_send(AudioMsg::Pcm(PcmChunk {
            pcm: std::mem::take(&mut accum),
            spec,
        }));
    }

    Ok(())
}

fn push_generic_audio_as_f32_interleaved(
    buf: GenericAudioBufferRef<'_>,
    channels: usize,
    out: &mut Vec<f32>,
) {
    let chans = buf.num_planes().max(1);
    let frames = buf.frames();
    if frames == 0 {
        return;
    }

    // Convert to f32 planar using Symphonia's copy helper and then interleave.
    //
    // The `GenericAudioBufferRef` supports `copy_to_slice_planar` in v0.6 through the generic module.
    let mut planar: Vec<Vec<f32>> = (0..chans).map(|_| vec![0.0; frames]).collect();
    let mut slices: Vec<&mut [f32]> = planar.iter_mut().map(|v| v.as_mut_slice()).collect();

    // If this fails to compile in future Symphonia changes, fallback to matching buffer variants.
    buf.copy_to_slice_planar(slices.as_mut_slice());

    let use_chans = channels.min(chans);
    for f in 0..frames {
        for ch in 0..use_chans {
            out.push(planar[ch][f]);
        }
    }
}

/// A minimal `MediaSource` adapter around [`ByteQueueReader`].
///
/// Symphonia requires `Read + Seek`. Our bytestream is forward-only, so `seek` is unsupported
/// except for `SeekFrom::Current(0)` (a "tell" request) which returns 0 (unknown position).
pub struct ByteQueueMediaSource {
    inner: ByteQueueReader,
    /// Best-effort byte offset observed by this adapter.
    ///
    /// This is used to support `SeekFrom::Current(0)` (tell) which Symphonia's probe may rely on.
    /// We do not support actual seeking, but reporting a monotonic position helps the probe avoid
    /// treating the stream as permanently at offset 0.
    pos: u64,
}

impl ByteQueueMediaSource {
    pub fn new(inner: ByteQueueReader) -> Self {
        Self { inner, pos: 0 }
    }
}

impl Read for ByteQueueMediaSource {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.pos = self.pos.saturating_add(n as u64);
        Ok(n)
    }
}

impl Seek for ByteQueueMediaSource {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            // Symphonia uses this as a "tell". Return our best-effort monotonic offset.
            SeekFrom::Current(0) => Ok(self.pos),
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "byte-queue media source is not seekable",
            )),
        }
    }
}

impl MediaSource for ByteQueueMediaSource {
    fn is_seekable(&self) -> bool {
        false
    }

    fn byte_len(&self) -> Option<u64> {
        None
    }
}

/// Blocking `MediaSource` adapter over a bounded async bytes channel.
///
/// The async side sends compressed bytes into a `tokio::mpsc::Sender<Bytes>`.
/// The decoder thread blocks waiting for new chunks when it needs more data.
/// EOF is signaled by closing the sender (receiver yields `None`).
pub struct AsyncBytesMediaSource {
    inner: EpochBytesRx,
    front: Option<bytes::Bytes>,
    front_off: usize,
    pos: u64,
}

impl AsyncBytesMediaSource {
    pub fn new(inner: EpochBytesRx) -> Self {
        Self {
            inner,
            front: None,
            front_off: 0,
            pos: 0,
        }
    }

    fn refill(&mut self) -> io::Result<bool> {
        if self.front.is_some() {
            return Ok(true);
        }

        // Block until we receive the next chunk or EOF.
        //
        // NOTE: This runs on the decoder thread (spawn_blocking), so blocking is acceptable.
        match self.inner.blocking_recv() {
            Some(b) => {
                if b.is_empty() {
                    // Skip empty chunks.
                    return Ok(self.refill()?);
                }
                self.front = Some(b);
                self.front_off = 0;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

impl Read for AsyncBytesMediaSource {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if !self.refill()? {
            return Ok(0);
        }

        let Some(front) = self.front.as_ref() else {
            return Ok(0);
        };

        let start = self.front_off.min(front.len());
        let avail = front.len().saturating_sub(start);
        if avail == 0 {
            self.front = None;
            self.front_off = 0;
            return self.read(buf);
        }

        let n = avail.min(buf.len());
        buf[..n].copy_from_slice(&front.slice(start..start + n));

        self.front_off = self.front_off.saturating_add(n);
        self.pos = self.pos.saturating_add(n as u64);

        if let Some(f) = self.front.as_ref() {
            if self.front_off >= f.len() {
                self.front = None;
                self.front_off = 0;
            }
        }

        Ok(n)
    }
}

impl Seek for AsyncBytesMediaSource {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Current(0) => Ok(self.pos),
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "async-bytes media source is not seekable",
            )),
        }
    }
}

impl MediaSource for AsyncBytesMediaSource {
    fn is_seekable(&self) -> bool {
        false
    }

    fn byte_len(&self) -> Option<u64> {
        None
    }
}

/// Convenience: convert a `ByteQueueReader` into a boxed `MediaSource` for epoch wiring.
pub fn epoch_media_source_from_byte_queue(
    reader: ByteQueueReader,
) -> Box<dyn MediaSource + Send + Sync> {
    Box::new(ByteQueueMediaSource::new(reader))
}

/// Convenience: convert a bounded async bytes receiver into a boxed `MediaSource` for epoch wiring.
pub fn epoch_media_source_from_async_bytes(
    bytes_rx: EpochBytesRx,
) -> Box<dyn MediaSource + Send + Sync> {
    Box::new(AsyncBytesMediaSource::new(bytes_rx))
}
