#![forbid(unsafe_code)]

use std::fmt;
use std::io::{Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::Stream;
use tokio::sync::mpsc;

use stream_download::Settings;
use stream_download::StreamDownload;
use stream_download::source::SourceStream;
use stream_download::storage::StorageProvider;
use stream_download_hls::VariantId;
use stream_download_hls::{HlsCommand, HlsStream, HlsStreamParams};

use symphonia::core::audio::layouts::CHANNEL_LAYOUT_STEREO;
use symphonia::core::codecs::audio::{AudioDecoder, AudioDecoderOptions};
use symphonia::core::formats::probe::Hint;
use symphonia::core::formats::{FormatOptions, FormatReader, SeekMode, SeekTo, TrackType};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::units::Time;
use symphonia::default::{get_codecs, get_probe};

#[cfg(feature = "rodio")]
use std::collections::VecDeque;
#[cfg(feature = "rodio")]
use std::time::Instant;

/// Audio output description for produced PCM.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AudioSpec {
    pub sample_rate: u32,
    pub channels: u16,
}

/// A chunk of interleaved `f32` PCM.
///
/// Invariant: `pcm.len() % spec.channels as usize == 0`.
#[derive(Clone, Debug)]
pub struct PcmChunk {
    pub spec: AudioSpec,
    pub pcm: Vec<f32>,
}

/// Commands sent to the audio worker/source.
#[derive(Clone, Debug)]
pub enum AudioCommand {
    /// Best-effort seek by time.
    Seek(Duration),

    /// Manual variant switch (HLS only).
    ///
    /// HTTP sources may ignore or return `AudioError::NotSupported`.
    SetHlsVariant { variant: VariantId },

    /// Return to ABR/auto selection (HLS only).
    ///
    /// HTTP sources may ignore or return `AudioError::NotSupported`.
    ClearHlsVariantOverride,
}

#[derive(Debug)]
pub enum AudioError {
    NotSupported(&'static str),
    InvalidPcm(&'static str),
    EndOfStream,
    Io(std::io::Error),
    Decode(String),
    Other(String),
}

impl fmt::Display for AudioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotSupported(msg) => write!(f, "not supported: {msg}"),
            Self::InvalidPcm(msg) => write!(f, "invalid pcm: {msg}"),
            Self::EndOfStream => write!(f, "end of stream"),
            Self::Io(e) => write!(f, "{e}"),
            Self::Decode(msg) => write!(f, "decode error: {msg}"),
            Self::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for AudioError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for AudioError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// Audio pipeline settings.
///
/// v1 does not implement resampling yet. Sample-rate conversion will be added later.
/// The pipeline should call a placeholder hook where resampling will eventually live.
#[derive(Clone, Debug)]
pub struct AudioSettings {
    /// Bounded queue capacity in chunks.
    ///
    /// The producer waits when full (no drops). The consumer is non-blocking.
    pub queue_capacity_chunks: usize,

    pub target_channels: u16,
    pub target_sample_rate: u32,
}

impl Default for AudioSettings {
    fn default() -> Self {
        Self {
            queue_capacity_chunks: 8,
            target_channels: 2,
            target_sample_rate: 48_000,
        }
    }
}

/// Trait-based source that owns/uses stream-download primitives internally and produces decoded PCM.
///
/// This trait is intentionally synchronous: it is driven by a worker task which can be implemented
/// via a blocking loop (e.g. `spawn_blocking`) or a dedicated synchronous runtime.
pub trait AudioSource: Send + 'static {
    fn output_spec(&self) -> Option<AudioSpec>;

    /// Produce the next chunk of PCM.
    ///
    /// - `Ok(Some(chunk))`: chunk produced.
    /// - `Ok(None)`: end-of-stream reached.
    /// - `Err(e)`: fatal error; stream will terminate.
    fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError>;

    /// Handle a command (best-effort).
    ///
    /// HLS sources should support variant switching.
    /// HTTP sources may ignore/return `NotSupported` for HLS-only commands.
    fn handle_command(&mut self, cmd: AudioCommand) -> Result<(), AudioError>;
}

struct SharedReaderMediaSource<P: StorageProvider> {
    inner: SharedReader<P>,
    len: Option<u64>,
    seek_enabled: Arc<AtomicBool>,
}

impl<P: StorageProvider> SharedReaderMediaSource<P> {
    fn new(inner: SharedReader<P>, seek_enabled: Arc<AtomicBool>, len: Option<u64>) -> Self {
        Self {
            inner,
            len,
            seek_enabled,
        }
    }
}

impl<P> MediaSource for SharedReaderMediaSource<P>
where
    P: StorageProvider + 'static,
{
    fn is_seekable(&self) -> bool {
        self.seek_enabled.load(Ordering::Relaxed)
    }

    fn byte_len(&self) -> Option<u64> {
        self.len
    }
}

impl<P> Read for SharedReaderMediaSource<P>
where
    P: StorageProvider + 'static,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "reader lock poisoned"))?;
        guard.read(buf)
    }
}

impl<P> Seek for SharedReaderMediaSource<P>
where
    P: StorageProvider + 'static,
{
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "reader lock poisoned"))?;
        guard.seek(pos)
    }
}

struct DecodeEngine {
    spec: Option<AudioSpec>,
    reader: Option<Box<dyn FormatReader>>,
    decoder: Option<Box<dyn AudioDecoder>>,
    track_id: Option<u32>,
    seek_enabled: Arc<AtomicBool>,
}

impl DecodeEngine {
    fn new() -> Self {
        Self {
            spec: None,
            reader: None,
            decoder: None,
            track_id: None,
            seek_enabled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn output_spec(&self) -> Option<AudioSpec> {
        self.spec
    }

    fn reset(&mut self) {
        self.spec = None;
        self.reader = None;
        self.decoder = None;
        self.track_id = None;
        self.seek_enabled.store(false, Ordering::Relaxed);
    }

    fn ensure_open<P>(&mut self, reader: SharedReader<P>) -> Result<(), AudioError>
    where
        P: StorageProvider + 'static,
    {
        if self.reader.is_some() && self.decoder.is_some() && self.track_id.is_some() {
            return Ok(());
        }

        let len = None;

        let media_src = Box::new(SharedReaderMediaSource::new(
            reader,
            Arc::clone(&self.seek_enabled),
            len,
        ));
        let mss = MediaSourceStream::new(media_src, Default::default());

        let hint = Hint::new();
        let probed = get_probe()
            .probe(
                &hint,
                mss,
                FormatOptions::default(),
                MetadataOptions::default(),
            )
            .map_err(|e| AudioError::Decode(e.to_string()))?;

        self.seek_enabled.store(true, Ordering::Relaxed);

        let reader = probed;

        let track = reader
            .default_track(TrackType::Audio)
            .ok_or_else(|| AudioError::Decode("no audio track".into()))?;

        let codec_params = track
            .codec_params
            .as_ref()
            .ok_or_else(|| AudioError::Decode("invalid codec params".into()))?
            .audio()
            .ok_or_else(|| AudioError::Decode("invalid audio codec params".into()))?;

        let decoder = get_codecs()
            .make_audio_decoder(&codec_params, &AudioDecoderOptions::default())
            .map_err(|e| AudioError::Decode(e.to_string()))?;

        let sample_rate = codec_params
            .sample_rate
            .ok_or_else(|| AudioError::Decode("missing sample rate".into()))?;

        let channels = codec_params
            .channels
            .as_ref()
            .cloned()
            .unwrap_or(CHANNEL_LAYOUT_STEREO);

        self.spec = Some(AudioSpec {
            sample_rate,
            channels: channels.count() as u16,
        });
        self.track_id = Some(track.id);
        self.decoder = Some(decoder);
        self.reader = Some(reader);

        Ok(())
    }

    fn next_chunk<P>(&mut self, reader: SharedReader<P>) -> Result<Option<PcmChunk>, AudioError>
    where
        P: StorageProvider + 'static,
    {
        self.ensure_open(Arc::clone(&reader))?;

        let Some(reader) = self.reader.as_mut() else {
            return Err(AudioError::Other(
                "decode engine: missing format reader".into(),
            ));
        };
        let Some(decoder) = self.decoder.as_mut() else {
            return Err(AudioError::Other("decode engine: missing decoder".into()));
        };

        let track_id = self
            .track_id
            .ok_or_else(|| AudioError::Other("decode engine: missing track id".into()))?;

        loop {
            match reader.next_packet() {
                Ok(Some(packet)) => {
                    if packet.track_id() != track_id {
                        continue;
                    }

                    let decoded = decoder
                        .decode(&packet)
                        .map_err(|e| AudioError::Decode(e.to_string()))?;

                    let frames = decoded.frames();
                    if frames == 0 {
                        continue;
                    }

                    let spec = self
                        .spec
                        .ok_or_else(|| AudioError::Other("decode engine: missing spec".into()))?;

                    let channels = spec.channels as usize;
                    let mut left = vec![0.0f32; frames];
                    let mut right = vec![0.0f32; frames];

                    let mut chans: [&mut [f32]; 2] = [&mut left, &mut right];
                    decoded.copy_to_slice_planar(&mut chans);

                    let mut interleaved = Vec::with_capacity(frames * channels);
                    if channels == 1 {
                        interleaved.extend_from_slice(&left[..frames]);
                    } else {
                        for i in 0..frames {
                            interleaved.push(left[i]);
                            interleaved.push(right[i]);
                        }
                    }

                    return Ok(Some(PcmChunk {
                        spec,
                        pcm: interleaved,
                    }));
                }
                Ok(None) => return Ok(None),
                Err(e) => return Err(AudioError::Decode(e.to_string())),
            }
        }
    }

    fn seek<P>(&mut self, reader: SharedReader<P>, position: Duration) -> Result<(), AudioError>
    where
        P: StorageProvider + 'static,
    {
        self.ensure_open(Arc::clone(&reader))?;

        let Some(format_reader) = self.reader.as_mut() else {
            return Err(AudioError::Other(
                "decode engine: missing format reader".into(),
            ));
        };
        let Some(decoder) = self.decoder.as_mut() else {
            return Err(AudioError::Other("decode engine: missing decoder".into()));
        };

        let time = Time::from(position.as_secs_f64());
        let r = format_reader.seek(
            SeekMode::Accurate,
            SeekTo::Time {
                time,
                track_id: self.track_id,
            },
        );

        match r {
            Ok(_) => {
                decoder.reset();
                Ok(())
            }
            Err(e) => Err(AudioError::Decode(e.to_string())),
        }
    }
}

type SharedReader<P> = Arc<Mutex<StreamDownload<P>>>;

/// High-level, non-blocking async stream of decoded PCM chunks.
///
/// Generic parameters exist to tie the type to a concrete `stream-download` stream type `S`
/// (e.g. `HlsStream`, `HttpStream`) and `StorageProvider` `P`, even though the implementation
/// is hidden behind a trait-based source.
pub struct AudioStream<S, P> {
    rx: mpsc::Receiver<Result<PcmChunk, AudioError>>,
    cmd_tx: mpsc::Sender<AudioCommand>,
    last_spec: Option<AudioSpec>,
    _phantom: PhantomData<fn() -> (S, P)>,
}

impl<S, P> AudioStream<S, P> {
    /// Create a new `AudioStream` from a trait-based source.
    pub async fn new(
        source: Box<dyn AudioSource>,
        settings: AudioSettings,
    ) -> Result<Self, AudioError> {
        let (tx, rx) =
            mpsc::channel::<Result<PcmChunk, AudioError>>(settings.queue_capacity_chunks);
        let (cmd_tx, cmd_rx) = mpsc::channel::<AudioCommand>(32);

        let last_spec = source.output_spec();

        tokio::spawn(drive_source(source, settings.clone(), tx, cmd_rx));

        Ok(Self {
            rx,
            cmd_tx,
            last_spec,
            _phantom: PhantomData,
        })
    }

    pub fn output_spec(&self) -> Option<AudioSpec> {
        self.last_spec
    }

    pub fn command_tx(&self) -> mpsc::Sender<AudioCommand> {
        self.cmd_tx.clone()
    }

    fn resample_placeholder(&mut self, _chunk: &mut PcmChunk, _settings: &AudioSettings) {
        todo!("rubato-based resampling will be integrated here")
    }
}

#[cfg(feature = "rodio")]
struct RodioBufferState {
    spec: Option<AudioSpec>,
    queue: VecDeque<f32>,
    ended: bool,
    error: Option<AudioError>,
    last_progress: Instant,
}

#[cfg(feature = "rodio")]
impl RodioBufferState {
    fn new() -> Self {
        Self {
            spec: None,
            queue: VecDeque::new(),
            ended: false,
            error: None,
            last_progress: Instant::now(),
        }
    }
}

/// `rodio` adapter: drives an `AudioStream` in a background task and exposes it as `rodio::Source`.
///
/// - Chunk sizes are arbitrary; this adapter buffers samples and feeds rodio one-by-one.
/// - If the internal buffer is empty, `next()` returns silence (0.0).
/// - If the stream ends, `next()` returns `None`.
#[cfg(feature = "rodio")]
pub struct RodioSourceAdapter<S, P> {
    state: Arc<Mutex<RodioBufferState>>,
    _phantom: PhantomData<fn() -> (S, P)>,
}

#[cfg(feature = "rodio")]
impl<S, P> RodioSourceAdapter<S, P>
where
    S: 'static,
    P: 'static,
{
    pub fn new(mut stream: AudioStream<S, P>) -> Self {
        let state = Arc::new(Mutex::new(RodioBufferState::new()));
        let state_clone = Arc::clone(&state);

        tokio::spawn(async move {
            use futures_util::StreamExt;

            while let Some(item) = stream.next().await {
                match item {
                    Ok(chunk) => {
                        let mut guard = state_clone.lock().expect("rodio buffer lock poisoned");
                        guard.spec = Some(chunk.spec);
                        guard.queue.extend(chunk.pcm);
                        guard.last_progress = Instant::now();
                    }
                    Err(e) => {
                        let mut guard = state_clone.lock().expect("rodio buffer lock poisoned");
                        guard.error = Some(e);
                        guard.ended = true;
                        break;
                    }
                }
            }

            let mut guard = state_clone.lock().expect("rodio buffer lock poisoned");
            guard.ended = true;
        });

        Self {
            state,
            _phantom: PhantomData,
        }
    }

    fn spec(&self) -> Option<AudioSpec> {
        self.state.lock().ok().and_then(|guard| guard.spec)
    }
}

#[cfg(feature = "rodio")]
impl<S, P> Iterator for RodioSourceAdapter<S, P>
where
    S: 'static,
    P: 'static,
{
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        let mut guard = self.state.lock().expect("rodio buffer lock poisoned");

        if guard.error.is_some() {
            // End the source on error.
            return None;
        }

        if let Some(sample) = guard.queue.pop_front() {
            return Some(sample);
        }

        if guard.ended {
            return None;
        }

        // Underrun: return silence.
        Some(0.0)
    }
}

#[cfg(feature = "rodio")]
impl<S, P> rodio::Source for RodioSourceAdapter<S, P>
where
    S: 'static,
    P: 'static,
{
    fn current_span_len(&self) -> Option<usize> {
        None
    }

    fn channels(&self) -> u16 {
        self.spec().map(|s| s.channels).unwrap_or(2)
    }

    fn sample_rate(&self) -> u32 {
        self.spec().map(|s| s.sample_rate).unwrap_or(48_000)
    }

    fn total_duration(&self) -> Option<Duration> {
        None
    }
}

impl<P> AudioStream<stream_download::http::HttpStream<reqwest::Client>, P>
where
    P: StorageProvider + 'static,
    P::Reader: Send,
{
    pub async fn new_http(
        url: url::Url,
        storage: P,
        stream_settings: Settings<stream_download::http::HttpStream<reqwest::Client>>,
        audio_settings: AudioSettings,
    ) -> Result<Self, AudioError> {
        let source = Box::new(HttpAudioSource::new(url, storage, stream_settings).await?);
        AudioStream::new(source, audio_settings).await
    }
}

impl<P> AudioStream<HlsStream, P>
where
    P: StorageProvider + 'static,
    P::Reader: Send,
{
    pub async fn new_hls(
        params: HlsStreamParams,
        storage: P,
        stream_settings: Settings<HlsStream>,
        audio_settings: AudioSettings,
    ) -> Result<Self, AudioError> {
        let source = Box::new(HlsAudioSource::new(params, storage, stream_settings).await?);
        AudioStream::new(source, audio_settings).await
    }
}

async fn drive_source(
    mut source: Box<dyn AudioSource>,
    settings: AudioSettings,
    tx: mpsc::Sender<Result<PcmChunk, AudioError>>,
    mut cmd_rx: mpsc::Receiver<AudioCommand>,
) {
    loop {
        while let Ok(cmd) = cmd_rx.try_recv() {
            let _ = source.handle_command(cmd);
        }

        let next = match tokio::task::spawn_blocking({
            let source = source;
            move || {
                let mut source = source;
                let res = source.next_chunk();
                (source, res)
            }
        })
        .await
        {
            Ok((returned_source, res)) => {
                source = returned_source;
                res
            }
            Err(join_err) => {
                let _ = tx.send(Err(AudioError::Other(join_err.to_string()))).await;
                break;
            }
        };

        match next {
            Ok(Some(chunk)) => {
                // v1: placeholder hook for future rubato-based resampling.
                // Will become a no-op or real resample depending on settings.
                let _ = settings.target_sample_rate;
                let _ = settings.target_channels;

                // Ensure basic invariant (frame alignment).
                if chunk.spec.channels == 0 {
                    let _ = tx
                        .send(Err(AudioError::InvalidPcm("channels must be > 0")))
                        .await;
                    break;
                }
                if chunk.pcm.len() % chunk.spec.channels as usize != 0 {
                    let _ = tx
                        .send(Err(AudioError::InvalidPcm("pcm must be frame-aligned")))
                        .await;
                    break;
                }

                if tx.send(Ok(chunk)).await.is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                break;
            }
        }
    }
}

struct HttpAudioSource<P>
where
    P: StorageProvider,
{
    reader: SharedReader<P>,
    engine: DecodeEngine,
}

impl<P> HttpAudioSource<P>
where
    P: StorageProvider + 'static,
{
    async fn new(
        url: url::Url,
        storage: P,
        settings: Settings<stream_download::http::HttpStream<reqwest::Client>>,
    ) -> Result<Self, AudioError> {
        let reader = StreamDownload::new_http(url, storage, settings)
            .await
            .map_err(|e| AudioError::Other(format!("{e:?}")))?;

        Ok(Self {
            reader: Arc::new(Mutex::new(reader)),
            engine: DecodeEngine::new(),
        })
    }
}

impl<P> AudioSource for HttpAudioSource<P>
where
    P: StorageProvider + 'static,
{
    fn output_spec(&self) -> Option<AudioSpec> {
        self.engine.output_spec()
    }

    fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError> {
        self.engine.next_chunk(Arc::clone(&self.reader))
    }

    fn handle_command(&mut self, cmd: AudioCommand) -> Result<(), AudioError> {
        match cmd {
            AudioCommand::Seek(pos) => self.engine.seek(Arc::clone(&self.reader), pos),
            AudioCommand::SetHlsVariant { .. } => Err(AudioError::NotSupported(
                "HLS variant switching is not supported for HTTP sources",
            )),
            AudioCommand::ClearHlsVariantOverride => Err(AudioError::NotSupported(
                "HLS variant switching is not supported for HTTP sources",
            )),
        }
    }
}

struct HlsAudioSource<P>
where
    P: StorageProvider,
{
    reader: SharedReader<P>,
    engine: DecodeEngine,
    hls_cmd_tx: mpsc::Sender<HlsCommand>,
}

impl<P> HlsAudioSource<P>
where
    P: StorageProvider + 'static,
{
    async fn new(
        params: HlsStreamParams,
        storage: P,
        settings: Settings<HlsStream>,
    ) -> Result<Self, AudioError> {
        let stream = HlsStream::create(params)
            .await
            .map_err(|e| AudioError::Other(format!("{e:?}")))?;

        let hls_cmd_tx = stream.command_sender();

        // NOTE: `Settings<S>` is not `Clone`; pass it by value.
        let reader = StreamDownload::from_stream(stream, storage, settings)
            .await
            .map_err(|e| AudioError::Other(format!("{e:?}")))?;

        Ok(Self {
            reader: Arc::new(Mutex::new(reader)),
            engine: DecodeEngine::new(),
            hls_cmd_tx,
        })
    }
}

impl<P> AudioSource for HlsAudioSource<P>
where
    P: StorageProvider + 'static,
{
    fn output_spec(&self) -> Option<AudioSpec> {
        self.engine.output_spec()
    }

    fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError> {
        // HLS-specific: on init/variant epoch changes, call `self.engine.reset()`.
        // Control plumbing will be added via a StorageWriter control tap.
        self.engine.next_chunk(Arc::clone(&self.reader))
    }

    fn handle_command(&mut self, cmd: AudioCommand) -> Result<(), AudioError> {
        match cmd {
            AudioCommand::Seek(pos) => self.engine.seek(Arc::clone(&self.reader), pos),
            AudioCommand::SetHlsVariant { variant } => {
                let _ = self.hls_cmd_tx.try_send(HlsCommand::SetVariant {
                    variant_id: variant,
                });
                Ok(())
            }
            AudioCommand::ClearHlsVariantOverride => {
                let _ = self.hls_cmd_tx.try_send(HlsCommand::ClearVariantOverride);
                Ok(())
            }
        }
    }
}

impl<S, P> Stream for AudioStream<S, P> {
    type Item = Result<PcmChunk, AudioError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut self.get_mut();

        match this.rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(chunk))) => {
                this.last_spec = Some(chunk.spec);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
        }
    }
}
