/*!
Public API types and traits for the `stream-download-audio` crate.

Design goals
------------
This crate follows the same high-level style as `stream-download-hls`:

- **Ordered data stream**: callers consume an ordered stream of `AudioMsg` (PCM + control).
- **Out-of-band events**: callers can subscribe to `PlayerEvent` for UI/telemetry/diagnostics.
- **No legacy pull API**: the old `SampleSource`/ring-buffer pull model is removed.

Implementation details (pipeline, backends, adapters, etc.) live in separate modules; `lib.rs`
should primarily re-export items from here.
*/

use std::time::Duration;

/// Local selection mode to decouple AudioOptions from HLS crate exports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionMode {
    Auto,
    /// Manual mode by variant index (0-based).
    Manual(usize),
}

/// Why the decoder lifecycle changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecoderLifecycleReason {
    /// Initial decoder creation for the first successfully probed stream.
    Initial,
    /// Decoder was recreated because init segment changed (e.g. due to variant switch).
    InitChanged,
    /// Decoder was recreated due to a seek (if/when supported).
    Seek,
    /// Decoder was recreated due to an explicit flush/reset (if/when supported).
    Flush,
}

/// Audio-level reason for HLS variant changes.
///
/// This is intentionally an enum (not a string) so callers can reliably branch on it
/// and tests can assert exact behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbrVariantChangeReason {
    /// Initial variant selection during startup (e.g. the first transition from "no variant yet"
    /// to the configured initial variant index).
    ///
    /// This is expected to happen before meaningful throughput measurements exist and should be
    /// treated differently in strict ABR-switch tests (typically filtered out).
    Initial,
    /// Automatic ABR decision based on throughput/buffer heuristics.
    Auto,
    /// Explicit manual selection by the user (by variant index).
    Manual,
    /// Unknown/unspecified reason.
    Unknown,
}

/// Basic PCM specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AudioSpec {
    pub sample_rate: u32,
    pub channels: u16,
}

/// Origin metadata for decoded audio.
///
/// This is best-effort: some backends can provide rich provenance (e.g. HLS segment boundaries),
/// while others may provide only coarse info or none at all.
///
/// The key requirement is that this metadata is **ordered with the PCM** (i.e., it describes where
/// the audio *came from*), so tests can assert "switch applied" by observing frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AudioOrigin {
    /// Decoded from an HLS media segment.
    Hls {
        /// Variant index (0-based).
        variant: usize,
        /// HLS media sequence number.
        sequence: u64,
    },
    /// Decoded from a non-segmented HTTP resource.
    ///
    /// `byte_offset` is best-effort and may be `None` when not tracked.
    Http { byte_offset: Option<u64> },
    /// Backend did not provide origin metadata.
    Unknown,
}

/// Decoded PCM frame batch.
///
/// `pcm` is interleaved f32 samples.
/// The `spec` describes the format of the samples in this frame batch.
///
/// `origin` is ordered with the samples and can be used for strict assertions in tests.
#[derive(Debug, Clone)]
pub struct AudioFrame {
    pub pcm: Vec<f32>,
    pub spec: AudioSpec,
    pub origin: AudioOrigin,
}

/// Ordered control messages emitted alongside audio frames.
///
/// These are part of the ordered `AudioMsg` stream (like `StreamControl` in `stream-download-hls`)
/// and are intended for strict, deterministic consumer logic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AudioControl {
    /// Output spec changed (e.g., after decoder initialization/probing).
    FormatChanged { spec: AudioSpec },

    /// End of stream reached (no more frames will be emitted).
    EndOfStream,
}

/// Ordered audio stream message (data + control).
///
/// This is the primary consumption API for `stream-download-audio`.
#[derive(Debug, Clone)]
pub enum AudioMsg {
    /// Decoded audio samples.
    Frame(AudioFrame),
    /// Ordered control boundary.
    Control(AudioControl),
}

/// High-level player events useful for UI/telemetry.
///
/// These events are out-of-band (not ordered with `AudioMsg`), and are best-effort.
/// For deterministic testing/consumption logic prefer the ordered `AudioMsg` stream.
#[derive(Debug, Clone)]
pub enum PlayerEvent {
    Started,

    /// HLS ABR variant changed (decision made by the HLS layer).
    ///
    /// This is the "decision" signal. It does NOT necessarily mean the first init/media bytes for
    /// the new variant have started flowing yet.
    VariantChanged {
        from: Option<usize>,
        to: usize,
        reason: AbrVariantChangeReason,
    },

    /// HLS init segment started for a specific variant.
    HlsInitStart {
        variant: usize,
    },

    /// HLS media segment started for a specific variant.
    HlsSegmentStart {
        variant: usize,
        sequence: u64,
    },

    /// Decoder lifecycle event (creation/recreation).
    DecoderInitialized {
        /// Variant index associated with the packet that triggered (re)initialization, if known.
        variant: Option<usize>,
        reason: DecoderLifecycleReason,
    },

    /// Output format changed.
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

/// Trait for inserting post-decode processing (effects) in the pipeline.
///
/// Processors should operate in-place on interleaved f32 frames.
pub trait AudioProcessor: Send + Sync {
    fn process(&self, pcm: &mut [f32], spec: AudioSpec) -> Result<(), String>;
}

/// Options for constructing an audio stream.
#[derive(Debug, Clone)]
pub struct AudioOptions {
    /// HLS selection mode selection (ignored for HTTP sources).
    pub selection_mode: SelectionMode,
    /// Target output sample rate of the audio session (resampling target).
    pub target_sample_rate: u32,
    /// Target output channels (e.g., 2 for stereo).
    pub target_channels: u16,
    /// Capacity of the ordered frame buffer in **frames** (not samples).
    ///
    /// This is analogous to `stream-download-hls::HlsSettings::prefetch_buffer_size`, but applies
    /// to decoded audio frames (PCM) rather than bytes.
    pub frame_buffer_capacity_frames: usize,
    /// Minimal interval between ABR switches when in Auto mode.
    pub abr_min_switch_interval: Duration,
    /// Hysteresis for up-switch decisions in ABR.
    pub abr_up_hysteresis_ratio: f32,
}

impl Default for AudioOptions {
    fn default() -> Self {
        Self {
            selection_mode: SelectionMode::Auto,
            target_sample_rate: 48_000,
            target_channels: 2,
            frame_buffer_capacity_frames: 8192, // ~170ms @ 48kHz
            abr_min_switch_interval: Duration::from_millis(4000),
            abr_up_hysteresis_ratio: 0.15,
        }
    }
}

impl AudioOptions {
    pub fn with_selection_mode(mut self, mode: SelectionMode) -> Self {
        self.selection_mode = mode;
        self
    }

    pub fn with_target_sample_rate(mut self, rate: u32) -> Self {
        self.target_sample_rate = rate;
        self
    }

    pub fn with_target_channels(mut self, channels: u16) -> Self {
        self.target_channels = channels;
        self
    }

    pub fn with_frame_buffer_capacity_frames(mut self, capacity: usize) -> Self {
        self.frame_buffer_capacity_frames = capacity;
        self
    }

    pub fn with_abr_min_switch_interval(mut self, interval: Duration) -> Self {
        self.abr_min_switch_interval = interval;
        self
    }

    pub fn with_abr_up_hysteresis_ratio(mut self, ratio: f32) -> Self {
        self.abr_up_hysteresis_ratio = ratio;
        self
    }
}
