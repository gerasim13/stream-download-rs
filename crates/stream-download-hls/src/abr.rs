/stream-download-rs/crates/stream-download-hls/src/abr.rs#L1-200
//! Adaptive Bitrate (ABR) controller.
//!
//! This module provides a simple abstraction around variant selection for HLS
//! streams. The goal for the PoC is to keep the logic intentionally minimal
//! while still making the design extensible for more advanced strategies
//! in the future.
//!
//! Design goals:
//! - Keep `AbrController` independent from the actual networking layer.
//! - Let higher layers provide playback/network metrics.
//! - Delegate the actual switching work to `HlsManager`.
//!
//! At this stage, the implementation is a stub: we define public types and
//! a minimal API, but the internal logic is intentionally left unimplemented.
//! This allows consumers to start integrating the API while the internals
//! are iterated on.

use crate::{HlsError, HlsManager, HlsResult, MasterPlaylist};

/// Basic playback / network metrics used for ABR decisions.
///
/// In a real implementation these values would be derived from:
/// - segment download times (for throughput estimation),
/// - buffer occupancy in seconds,
/// - decoder/presentation stats (dropped frames, stalls, etc.).
#[derive(Debug, Clone)]
pub struct PlaybackMetrics {
    /// Estimated available throughput in bits per second.
    pub throughput_bps: u64,
    /// How many seconds of media are currently buffered.
    pub buffer_seconds: f32,
    /// Number of dropped frames or similar quality metric.
    pub dropped_frames: u32,
}

/// Configuration for a basic ABR strategy.
///
/// This is intentionally small. The idea is to expose just enough knobs
/// for experimentation in the PoC without committing to a complex API.
#[derive(Debug, Clone)]
pub struct AbrConfig {
    /// Minimum buffer (in seconds) below which the controller will be
    /// more aggressive in down-switching.
    pub min_buffer_for_up_switch: f32,
    /// Safety factor applied to throughput when selecting a variant.
    /// For example, if set to 0.8, and throughput is 5 Mbps, ABR will
    /// try to select a variant with bandwidth <= 4 Mbps.
    pub throughput_safety_factor: f32,
}

impl Default for AbrConfig {
    fn default() -> Self {
        Self {
            min_buffer_for_up_switch: 10.0,
            throughput_safety_factor: 0.8,
        }
    }
}

/// Simple ABR controller that works together with `HlsManager`.
///
/// Responsibilities:
/// - Initialize from a master playlist and initial variant index.
/// - Keep track of the current variant index.
/// - Expose a `maybe_switch` method that decides whether to change
///   the variant based on incoming metrics.
///
/// This type does not perform any network I/O by itself: it calls into
/// `HlsManager` for actual variant switching.
#[derive(Debug)]
pub struct AbrController {
    manager: HlsManager,
    config: AbrConfig,
    current_variant_index: usize,
}

impl AbrController {
    /// Create a new ABR controller around an existing `HlsManager`.
    ///
    /// For now we require the caller to supply the initial variant index.
    /// In the future we may add helpers that pick the initial index based
    /// on advertised bandwidths in the master playlist.
    pub fn new(manager: HlsManager, config: AbrConfig, initial_variant_index: usize) -> Self {
        Self {
            manager,
            config,
            current_variant_index: initial_variant_index,
        }
    }

    /// Get a reference to the underlying `HlsManager`.
    pub fn manager(&self) -> &HlsManager {
        &self.manager
    }

    /// Get a mutable reference to the underlying `HlsManager`.
    ///
    /// This is useful for calling methods that are not directly proxied
    /// by `AbrController`, such as low-level playlist operations.
    pub fn manager_mut(&mut self) -> &mut HlsManager {
        &mut self.manager
    }

    /// Get the current variant index known to the controller.
    pub fn current_variant_index(&self) -> usize {
        self.current_variant_index
    }

    /// Initialize the controller from the master playlist.
    ///
    /// In a full implementation this might:
    /// - validate `initial_variant_index` against the playlist,
    /// - potentially adjust the initial index based on bandwidths
    ///   and the advertised information in `MasterPlaylist`.
    ///
    /// For now this method simply performs basic validation.
    pub fn validate_initial_variant(&self, master: &MasterPlaylist) -> HlsResult<()> {
        if master.variants.is_empty() {
            return Err(HlsError::Message(
                "master playlist has no variants".to_string(),
            ));
        }

        if self.current_variant_index >= master.variants.len() {
            return Err(HlsError::Message(format!(
                "initial variant index {} out of range ({} variants)",
                self.current_variant_index,
                master.variants.len()
            )));
        }

        Ok(())
    }

    /// Potentially switch to a different variant based on the provided
    /// `metrics`.
    ///
    /// The logic is intentionally left as a stub for now. The method
    /// returns `Ok(())` without changing anything, so callers can already
    /// wire up the control flow while the real ABR algorithm is still
    /// under development.
    ///
    /// Expected future behavior:
    /// - Inspect `master` and `metrics`.
    /// - Choose a new variant index (which may be the same as the current).
    /// - If different, call into `HlsManager` to perform the switch and
    ///   update `current_variant_index`.
    pub async fn maybe_switch(
        &mut self,
        _master: &MasterPlaylist,
        _metrics: &PlaybackMetrics,
    ) -> HlsResult<()> {
        // Stub implementation:
        // - Do nothing and keep the current variant.
        // - Return Ok so the control flow can be tested by callers.
        Ok(())
    }
}
