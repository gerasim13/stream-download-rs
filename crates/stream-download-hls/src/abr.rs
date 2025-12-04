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

use async_trait::async_trait;

use crate::model::{VariantId, VariantStream};
use crate::{HlsError, HlsResult, MediaStream, SegmentData};

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
pub struct AbrController<S: MediaStream> {
    stream: S,
    config: AbrConfig,
    /// The variant that the controller is currently targeting.
    current_variant_id: Option<VariantId>,
    initial_variant_index: usize,
}

impl<S: MediaStream> AbrController<S> {
    /// Create a new ABR controller that wraps a `MediaStream` implementation.
    ///
    /// The controller will use the provided `stream` to fetch segments and will
    /// add adaptive bitrate logic on top of it.
    ///
    /// # Arguments
    /// * `stream`: The underlying `MediaStream` (e.g., an `HlsManager`).
    /// * `config`: Configuration for the ABR algorithm.
    /// * `initial_variant_index`: The index of the variant to start with.
    pub fn new(stream: S, config: AbrConfig, initial_variant_index: usize) -> Self {
        Self {
            stream,
            config,
            current_variant_id: None,
            initial_variant_index,
        }
    }

    /// Get a reference to the underlying `MediaStream`.
    pub fn inner_stream(&self) -> &S {
        &self.stream
    }

    /// Get a mutable reference to the underlying `MediaStream`.
    pub fn inner_stream_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// Get the ID of the variant currently targeted by the controller.
    pub fn current_variant_id(&self) -> Option<VariantId> {
        self.current_variant_id
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
    /// - Inspect `self.stream.variants()` and `metrics`.
    /// - Choose a new variant ID.
    /// - If different, call `self.stream.select_variant()` and update `self.current_variant_id`.
    pub async fn maybe_switch(&mut self, _metrics: &PlaybackMetrics) -> HlsResult<()> {
        // Stub implementation:
        // - Do nothing and keep the current variant.
        // - Return Ok so the control flow can be tested by callers.
        Ok(())
    }
}

#[async_trait]
impl<S: MediaStream + Send + Sync> MediaStream for AbrController<S> {
    async fn init(&mut self) -> HlsResult<()> {
        self.stream.init().await?;

        // After init, the variants are available. Let's select the initial one.
        // We get the id first to solve a borrow checker issue, since `select_variant`
        // needs a mutable borrow of `self.stream` while the variant list is also
        // being borrowed.
        let initial_variant_id = self
            .stream
            .variants()
            .get(self.initial_variant_index)
            .map(|v| v.id);

        if let Some(id) = initial_variant_id {
            self.stream.select_variant(id).await?;
            self.current_variant_id = Some(id);
            Ok(())
        } else if self.stream.variants().is_empty() {
            // This is a valid state for master playlists with no variants.
            Ok(())
        } else {
            Err(HlsError::msg(format!(
                "initial_variant_index {} is out of bounds for {} variants",
                self.initial_variant_index,
                self.stream.variants().len()
            )))
        }
    }

    fn variants(&self) -> &[VariantStream] {
        self.stream.variants()
    }

    async fn select_variant(&mut self, variant_id: VariantId) -> HlsResult<()> {
        // an ABR controller should allow manual override of the variant.
        self.current_variant_id = Some(variant_id);
        self.stream.select_variant(variant_id).await
    }

    async fn next_segment(&mut self) -> HlsResult<Option<SegmentData>> {
        // In a real player, metrics would be collected from the playback loop
        // (buffer level, download speed, etc.) and passed here.
        let metrics = PlaybackMetrics {
            throughput_bps: 5_000_000,
            buffer_seconds: 30.0,
            dropped_frames: 0,
        };

        // 1. Run the ABR logic to decide if we should switch streams.
        // The `maybe_switch` method will internally call `self.stream.select_variant()`.
        self.maybe_switch(&metrics).await?;

        // 2. Fetch the next segment from the underlying stream.
        self.stream.next_segment().await
    }
}
