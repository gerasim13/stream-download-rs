//! Settings for `stream-download-audio`.
//!
//! This module defines the `AudioSettings` struct that controls audio pipeline
//! behavior: target sample rate, channels, buffer sizes, and resampling quality.
//!
//! Backend-specific settings (HLS, HTTP) are passed separately via their respective
//! settings types (`HlsSettings`, `Settings<HttpStream>`, etc.).

/// Settings for audio pipeline configuration.
#[derive(Debug, Clone)]
pub struct AudioSettings {
    // ----------------------------
    // Output audio format
    // ----------------------------
    /// Target output sample rate (Hz).
    /// All decoded audio will be resampled to this rate.
    /// Default: 48000 Hz.
    pub target_sample_rate: u32,

    /// Target output channels (e.g., 2 for stereo, 1 for mono).
    /// Default: 2 (stereo).
    pub target_channels: u16,

    // ----------------------------
    // Buffer configuration
    // ----------------------------
    /// PCM ring buffer capacity in frames (frame = samples for all channels).
    /// Larger values improve resilience to underruns at cost of latency.
    /// Default: 8192 frames (~170ms @ 48kHz stereo).
    pub ring_capacity_frames: usize,

    // ----------------------------
    // Resampling configuration
    // ----------------------------
    /// Resampling quality.
    /// Higher quality uses more CPU but produces better audio.
    /// Default: Medium.
    pub resampling_quality: ResamplingQuality,
}

/// Resampling quality levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResamplingQuality {
    /// Fastest resampling, lower quality (good for testing).
    Low,
    /// Balanced quality and performance (default).
    Medium,
    /// Highest quality, more CPU usage.
    High,
}

impl Default for AudioSettings {
    fn default() -> Self {
        Self {
            target_sample_rate: 48_000,
            target_channels: 2,
            ring_capacity_frames: 8192, // ~170ms @ 48kHz
            resampling_quality: ResamplingQuality::Medium,
        }
    }
}

impl AudioSettings {
    /// Create default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set target sample rate.
    pub fn with_target_sample_rate(mut self, rate: u32) -> Self {
        self.target_sample_rate = rate;
        self
    }

    /// Set target channels.
    pub fn with_target_channels(mut self, channels: u16) -> Self {
        self.target_channels = channels;
        self
    }

    /// Set ring buffer capacity in frames.
    pub fn with_ring_capacity_frames(mut self, capacity: usize) -> Self {
        self.ring_capacity_frames = capacity;
        self
    }

    /// Set resampling quality.
    pub fn with_resampling_quality(mut self, quality: ResamplingQuality) -> Self {
        self.resampling_quality = quality;
        self
    }
}
