use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::Stream;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::decode::byte_queue::ByteQueue;
use crate::decode::symphonia_decoder::{self, EpochMsg};
use crate::source::{self, BoxAudioSource, SourceControl, SourceMsg};
use crate::types::{
    AudioCommand, AudioControl, AudioDecodeOptions, AudioMsg, AudioSpec, DecoderLifecycleReason,
};

/// High-level source selector for [`AudioDecodeStream`].
///
/// Internally this is converted into a boxed [`source::AudioSource`] implementation so the decode
/// pipeline is backend-agnostic.
#[derive(Debug, Clone)]
pub enum AudioSource {
    /// Progressive HTTP audio (mp3/aac/flac/…).
    Http { url: Url },
    /// HLS master playlist.
    Hls {
        url: Url,
        hls_settings: stream_download_hls::HlsSettings,
        /// Optional storage root for persistent caching (segments/playlists/keys).
        storage_root: Option<std::path::PathBuf>,
    },
}

impl AudioSource {
    fn into_boxed_source(self) -> BoxAudioSource {
        match self {
            AudioSource::Http { url } => Box::new(source::http::HttpAudioSource::new_default(url)),
            AudioSource::Hls {
                url,
                hls_settings,
                storage_root,
            } => Box::new(source::hls::HlsAudioSource::new(
                url,
                hls_settings,
                storage_root,
            )),
        }
    }
}

/// Iterator-style (async `Stream`) audio decoder.
///
/// # Contract
/// - Consumers iterate an ordered stream of [`AudioMsg`].
/// - [`AudioMsg::Control`] boundaries are ordered relative to [`AudioMsg::Pcm`].
/// - Backpressure:
///   - output is delivered through a bounded channel sized in *decoded samples* (not bytes),
///     so slow consumers will naturally slow down decode.
///
/// # Notes
/// This is intentionally a **skeleton** that will be filled in during the rewrite:
/// - the old `AudioStream` / `PipelineRunner` approach is removed.
/// - this type becomes the primary API for tests and production adapters (rodio/cpal).
/// Iterator-style (async `Stream`) audio decoder.
pub struct AudioDecodeStream {
    rx: mpsc::Receiver<AudioMsg>,
    cmd_tx: mpsc::Sender<AudioCommand>,
    cancel: CancellationToken,
}

impl AudioDecodeStream {
    /// Create a decoder stream from a generic [`AudioSource`].
    pub async fn new(source: AudioSource, opts: AudioDecodeOptions) -> io::Result<Self> {
        let src = source.into_boxed_source();
        Self::new_with_source(src, opts).await
    }

    /// Create a decoder stream from a boxed source implementation.
    ///
    /// This is the core constructor used by `new_http/new_hls/new`.
    pub async fn new_with_source(
        mut src: BoxAudioSource,
        opts: AudioDecodeOptions,
    ) -> io::Result<Self> {
        // Ordered decoded output (controls + PCM). Bounded to provide consumer backpressure.
        let (msg_tx, rx) = mpsc::channel::<AudioMsg>(output_capacity_from_opts(&opts));

        // Command side-channel (public API).
        // Bounded to avoid unbounded growth if a caller spams commands.
        let (cmd_tx, cmd_rx) = mpsc::channel::<AudioCommand>(32);

        let cancel = CancellationToken::new();

        // Decoder thread orchestration:
        // - async side sends epoch start messages with a fresh epoch `MediaSource`
        // - blocking Symphonia thread probes/decodes and emits ordered `AudioMsg` (controls + PCM)
        let (epoch_tx, epoch_rx) = std::sync::mpsc::channel::<EpochMsg>();
        let _decoder_task = symphonia_decoder::spawn_decoder_thread(epoch_rx, msg_tx.clone());

        // Inject commands into the source if it is a controllable type.
        //
        // We avoid relying on `Box::downcast` (which requires `Any` on the box itself).
        // Instead, the `AudioSource` trait exposes `as_any_mut()` for downcasting.
        if src.supports_commands() {
            if let Some(hls) = src
                .as_any_mut()
                .downcast_mut::<source::hls::HlsAudioSource>()
            {
                hls.cmd_rx = Some(cmd_rx);
            } else {
                // Source claimed it supports commands but isn't a known controllable type.
                // Drop the receiver to avoid deadlock and proceed without control.
                drop(cmd_rx);
            }
        } else {
            drop(cmd_rx);
        }

        let cancel_bg = cancel.clone();
        tokio::spawn(async move {
            let mut s = src.into_stream();

            // IMPORTANT:
            // Do NOT start the initial decoder epoch immediately.
            //
            // If we start the epoch before any bytes arrive, Symphonia probe will observe EOF at
            // 0 bytes (or "no suitable format reader"), which is exactly the failure we saw in tests.
            //
            // Instead, lazily start the epoch on the first incoming bytes (HTTP) or on the first
            // HLS init boundary (HLS).
            let mut epoch_started = false;

            // Current epoch byte queue (created eagerly, but the decoder won't see it until we
            // explicitly start the epoch).
            let mut current_epoch = ByteQueue::new(opts.max_buffered_bytes.get());
            let mut current_writer = current_epoch.writer();

            let output_spec = AudioSpec {
                sample_rate: opts.target_sample_rate,
                channels: opts.target_channels,
            };

            while let Some(item) = s.next().await {
                if cancel_bg.is_cancelled() {
                    current_writer.close();
                    break;
                }

                match item {
                    Ok(SourceMsg::Data(bytes)) => {
                        // Lazily start the initial epoch on the first bytes.
                        if !epoch_started {
                            epoch_started = true;
                            let _ = epoch_tx.send(EpochMsg::StartEpoch {
                                source: symphonia_decoder::epoch_media_source_from_byte_queue(
                                    current_epoch.reader(),
                                ),
                                reason: DecoderLifecycleReason::Initial,
                                output_spec,
                                pcm_chunk_frames: opts.pcm_chunk_frames.get(),
                            });
                        }

                        // Backpressure is enforced by the bounded ByteQueue.
                        // NOTE: `push` is blocking; we will move this to a dedicated blocking task
                        // or make the queue async-aware as a follow-up.
                        if let Err(_e) = current_writer.push(bytes) {
                            current_writer.close();
                            break;
                        }
                    }
                    Ok(SourceMsg::Control(ctrl)) => {
                        // Forward ordered boundary downstream (tests/consumers).
                        if let Some(ac) = map_source_control_to_audio_control(ctrl.clone()) {
                            let _ = msg_tx.send(AudioMsg::Control(ac)).await;
                        }

                        // Epoch switching:
                        // When we observe a new init start, we must start a new compressed-byte epoch
                        // so bytes across init segments never mix (codec/container may change).
                        if matches!(ctrl, SourceControl::HlsInitStart { .. }) {
                            // Close current epoch to unblock decoder, then start a new one.
                            current_writer.close();

                            current_epoch = ByteQueue::new(opts.max_buffered_bytes.get());
                            current_writer = current_epoch.writer();

                            // Start (or switch) decoder epoch for the new init.
                            //
                            // If no epoch has been started yet (e.g. HLS where init boundary comes
                            // before first media bytes), this is the initial decoder open.
                            let was_started = epoch_started;
                            epoch_started = true;
                            let reason = if was_started {
                                DecoderLifecycleReason::InitChanged
                            } else {
                                DecoderLifecycleReason::Initial
                            };

                            let _ = epoch_tx.send(EpochMsg::StartEpoch {
                                source: symphonia_decoder::epoch_media_source_from_byte_queue(
                                    current_epoch.reader(),
                                ),
                                reason,
                                output_spec,
                                pcm_chunk_frames: opts.pcm_chunk_frames.get(),
                            });
                        }
                    }
                    Ok(SourceMsg::EndOfStream) => {
                        current_writer.close();
                        break;
                    }
                    Err(_e) => {
                        // TODO: propagate a structured ordered error control once we define it.
                        current_writer.close();
                        break;
                    }
                }
            }

            // Notify decoder thread that no more epochs will arrive.
            let _ = epoch_tx.send(EpochMsg::EndOfStream);

            // Best-effort ordered termination signal is emitted by the decoder thread.
        });

        Ok(Self { rx, cmd_tx, cancel })
    }

    /// Create a decoder stream for progressive HTTP audio.
    pub async fn new_http(url: Url, opts: AudioDecodeOptions) -> io::Result<Self> {
        Self::new_with_source(
            Box::new(source::http::HttpAudioSource::new_default(url)),
            opts,
        )
        .await
    }

    /// Create a decoder stream for HLS audio.
    pub async fn new_hls(
        url: Url,
        hls_settings: stream_download_hls::HlsSettings,
        opts: AudioDecodeOptions,
        storage_root: Option<std::path::PathBuf>,
    ) -> io::Result<Self> {
        Self::new_with_source(
            Box::new(source::hls::HlsAudioSource::new(
                url,
                hls_settings,
                storage_root,
            )),
            opts,
        )
        .await
    }

    /// Get a sender for the command side-channel.
    ///
    /// This is the public control-plane for the stream (e.g. manual HLS variant switching).
    pub fn commands(&self) -> mpsc::Sender<AudioCommand> {
        self.cmd_tx.clone()
    }

    /// Request cancellation of the background pipeline.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Await next ordered message (convenience wrapper around the `Stream` impl).
    pub async fn next_msg(&mut self) -> Option<AudioMsg> {
        self.rx.recv().await
    }
}

impl Stream for AudioDecodeStream {
    type Item = AudioMsg;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_recv(cx)
    }
}

/// Compute output channel capacity in “messages” based on decoded sample buffering preferences.
///
/// We model output backpressure in **decoded samples**, but the channel is message-based.
/// For now we use a conservative mapping:
/// - assume each `Pcm` message carries roughly `pcm_chunk_frames * channels` samples,
/// - then allocate enough messages to cover `max_buffered_samples`.
fn output_capacity_from_opts(opts: &AudioDecodeOptions) -> usize {
    let channels = opts.target_channels.max(1) as usize;
    let chunk_samples = opts.pcm_chunk_frames.get().saturating_mul(channels).max(1);

    let max_samples = opts.max_buffered_samples.get().max(1);
    let msgs = (max_samples + chunk_samples - 1) / chunk_samples;

    // Ensure at least 1 so stream wiring always works.
    msgs.max(1)
}

fn map_source_control_to_audio_control(ctrl: SourceControl) -> Option<AudioControl> {
    match ctrl {
        SourceControl::HlsInitStart { id } => Some(AudioControl::HlsInitStart { id }),
        SourceControl::HlsInitEnd { id } => Some(AudioControl::HlsInitEnd { id }),
        SourceControl::HlsSegmentStart { id } => Some(AudioControl::HlsSegmentStart { id }),
        SourceControl::HlsSegmentEnd { id } => Some(AudioControl::HlsSegmentEnd { id }),
    }
}

#[allow(dead_code)]
fn _assert_nonzero_sizes(_opts: &AudioDecodeOptions) {
    // Keep a single place to sanity-check we never allow zero, even if defaults change.
    let _a: NonZeroUsize = _opts.max_buffered_bytes;
    let _b: NonZeroUsize = _opts.max_buffered_samples;
    let _c: NonZeroUsize = _opts.pcm_chunk_frames;
}
