/*!
Public API types and traits for the `stream-download-audio` crate.

This module defines the stable, high-level interfaces exposed to users of the
crate. Implementation details (pipeline, backends, adapters, etc.) live in
separate modules; `lib.rs` should primarily re-export items from here.
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

/// Trait representing a pull-based source of interleaved f32 PCM frames.
///
/// This is the lowest common denominator for integrating with custom players.
pub trait SampleSource: Send {
    /// Fill `out` with interleaved f32 samples. Returns the number of f32 written.
    /// Implementations should be non-blocking or block for short periods.
    fn read_interleaved(&mut self, out: &mut [f32]) -> usize;

    /// Current PCM spec.
    fn spec(&self) -> AudioSpec;

    /// Optional seek for VOD sources. Default: unsupported.
    fn seek(&mut self, _to: Duration) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "seek not supported",
        ))
    }

    /// Whether the source reached end-of-stream.
    fn is_eof(&self) -> bool {
        false
    }
}

/// High-level player events useful for UI/telemetry.
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
    ///
    /// This is an "application" signal: once you observe this for `variant=to`, the pipeline has
    /// actually transitioned into fetching/consuming the new variant's init/media boundaries.
    HlsInitStart {
        variant: usize,
    },

    /// HLS media segment started for a specific variant.
    ///
    /// This is the strongest signal for strict ABR assertions because it is emitted at segment
    /// boundaries (i.e., when we actually start consuming media for that variant).
    HlsSegmentStart {
        variant: usize,
        sequence: u64,
    },

    /// Decoder lifecycle event (creation/recreation).
    ///
    /// This replaces the previous `VariantSwitched` event, which was ambiguous: it was emitted
    /// during decoder initialization (often due to init changes) and did not strictly imply that
    /// the underlying HLS variant selection had changed.
    DecoderInitialized {
        /// Variant index associated with the packet that triggered (re)initialization, if known.
        variant: Option<usize>,
        reason: DecoderLifecycleReason,
    },

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
    /// PCM ring buffer capacity in frames (frame = samples_per_channel for all channels).
    pub ring_capacity_frames: usize,
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
            ring_capacity_frames: 8192, // ~170ms @ 48kHz
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

    pub fn with_ring_capacity_frames(mut self, capacity: usize) -> Self {
        self.ring_capacity_frames = capacity;
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
