//! Public, minimal types for the iterator-style audio decoding API.
//!
//! This module intentionally keeps the surface area small and focused on:
//! - ordered consumption (`AudioMsg`),
//! - deterministic boundaries (`AudioControl`),
//! - explicit, meaningful buffering/backpressure configuration (`AudioDecodeOptions`),
//! - and a **command API** for controlling playback (e.g. manual HLS variant switching).
//!
//! No legacy/pull APIs are provided.

use std::num::NonZeroUsize;
use std::time::Duration;

/// Basic PCM specification for emitted samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AudioSpec {
    pub sample_rate: u32,
    pub channels: u16,
}

/// A batch of decoded interleaved f32 PCM samples.
///
/// Invariants:
/// - `pcm.len()` is a multiple of `spec.channels` (unless `channels == 0`, which is invalid).
/// - Samples are interleaved: for stereo it's `L R L R ...`.
#[derive(Debug, Clone)]
pub struct PcmChunk {
    pub pcm: Vec<f32>,
    pub spec: AudioSpec,
}

/// Why the decoder lifecycle changed.
///
/// This is emitted as ordered control so tests/consumers can assert exact behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecoderLifecycleReason {
    /// Initial decoder creation for the first successfully probed stream.
    Initial,
    /// Decoder was recreated because the init segment changed (e.g. variant switch).
    InitChanged,
    /// Decoder was recreated due to an explicit flush/reset (future).
    Flush,
    /// Decoder was recreated due to a seek (future).
    Seek,
}

/// HLS chunk identity as observed by this crate.
///
/// Note:
/// - `variant` is expected to be stable and deterministic.
/// - `sequence` is optional because the `stream-download` ordered control protocol does not
///   currently carry HLS sequence numbers; those exist in out-of-band events.
///   We intentionally avoid relying on out-of-band events for deterministic "applied" checks.
///   Tests should primarily assert on `variant`, and treat `sequence` as best-effort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HlsChunkId {
    pub variant: usize,
    pub sequence: Option<u64>,
}

/// Ordered control messages emitted alongside decoded PCM.
///
/// These are **ordered** relative to `AudioMsg::Pcm` and are intended for strict testing
/// and deterministic consumer logic (similar to `StreamControl` in `stream-download`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AudioControl {
    /// Decoder initialized (or re-initialized).
    DecoderInitialized {
        reason: DecoderLifecycleReason,
    },

    /// Output format changed. This should be emitted after initialization and whenever the
    /// decoded output spec changes.
    FormatChanged {
        spec: AudioSpec,
    },

    /// HLS init segment boundary.
    HlsInitStart {
        id: HlsChunkId,
    },
    HlsInitEnd {
        id: HlsChunkId,
    },

    /// HLS media segment boundary.
    ///
    /// This is the strongest ordered signal for "switch applied" assertions:
    /// once a consumer observes `HlsSegmentStart { id.variant = X, .. }`, the pipeline has
    /// applied variant X at a media boundary (relative to the data flow).
    HlsSegmentStart {
        id: HlsChunkId,
    },
    HlsSegmentEnd {
        id: HlsChunkId,
    },

    /// End of stream reached (no more PCM will be emitted).
    EndOfStream,
}

/// Ordered stream message returned by [`crate::AudioDecodeStream`].
#[derive(Debug, Clone)]
pub enum AudioMsg {
    /// Ordered control boundary.
    Control(AudioControl),
    /// Decoded PCM chunk.
    Pcm(PcmChunk),
}

/// Commands that can be sent to an [`crate::AudioDecodeStream`].
///
/// This is a **public** control plane for your player/application.
/// The most important initial capability is manual HLS variant switching.
///
/// Notes:
/// - Commands are *not* ordered relative to `AudioMsg`; they are a side-channel.
/// - Ordered confirmation that a command took effect must be observed via `AudioMsg::Control`,
///   e.g. `AudioControl::HlsInitStart/HlsSegmentStart` for the target variant and
///   `AudioControl::DecoderInitialized { reason: InitChanged }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AudioCommand {
    /// Request a manual HLS variant switch by variant index (0-based in the master playlist order).
    ///
    /// The audio pipeline must:
    /// - initiate a switch in the underlying HLS layer,
    /// - start a new init epoch when the init segment for the new variant begins,
    /// - recreate the decoder if the codec/container changes,
    /// - and continue emitting PCM.
    ///
    /// Ordered confirmation should be observed via:
    /// - `AudioControl::HlsInitStart { id.variant = target }`
    /// - `AudioControl::DecoderInitialized { reason: InitChanged }` (if init differs)
    /// - `AudioControl::HlsSegmentStart { id.variant = target }`
    SetHlsVariant { variant: usize },
}

/// Configuration for decoding and buffering.
///
/// The design goal is to avoid "mysterious fixed buffer sizes".
/// Capacities are expressed in meaningful units:
/// - bytes buffered before the decoder (`max_buffered_bytes`)
/// - decoded samples buffered after the decoder (`max_buffered_samples`)
#[derive(Debug, Clone)]
pub struct AudioDecodeOptions {
    /// Target output sample rate for emitted PCM.
    ///
    /// Today this is used as "desired output spec". Resampling may be added later; currently
    /// streams that don't match may still be emitted as-is depending on decoder behavior.
    pub target_sample_rate: u32,

    /// Target output channels (e.g. 2 for stereo).
    ///
    /// Today this is used as "desired output spec". Channel mixing may be added later; currently
    /// streams that don't match may still be emitted as-is depending on decoder behavior.
    pub target_channels: u16,

    /// Maximum number of compressed bytes to buffer between the network/HLS source and the decoder.
    ///
    /// This is the primary backpressure control for upstream downloads.
    pub max_buffered_bytes: NonZeroUsize,

    /// Maximum number of decoded **interleaved f32 samples** buffered for consumers.
    ///
    /// This bounds memory when consumers (rodio/cpal) are slower than decode.
    pub max_buffered_samples: NonZeroUsize,

    /// Preferred size of emitted PCM chunks, in sample-frames (not samples).
    ///
    /// One sample-frame is a set of samples across all channels at a single time point.
    /// For stereo: 1 frame = 2 samples.
    pub pcm_chunk_frames: NonZeroUsize,

    /// Upper bound on how long the decoder thread should wait while probing for an initial format
    /// before re-trying (used to avoid busy-spins while still supporting streaming).
    pub probe_retry_interval: Duration,
}

impl Default for AudioDecodeOptions {
    fn default() -> Self {
        Self {
            target_sample_rate: 48_000,
            target_channels: 2,
            // 8 MiB compressed buffer (enough for multiple segments, small enough to provide pressure).
            max_buffered_bytes: NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
            // 2 seconds @ 48kHz stereo = 48_000 * 2 * 2 = 192_000 samples.
            max_buffered_samples: NonZeroUsize::new(192_000).unwrap(),
            // ~20ms @ 48kHz = 960 frames. Rounded.
            pcm_chunk_frames: NonZeroUsize::new(1024).unwrap(),
            probe_retry_interval: Duration::from_millis(5),
        }
    }
}
