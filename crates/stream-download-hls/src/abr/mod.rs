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

use self::bandwidth_estimator::BandwidthEstimator;
use crate::model::{VariantId, VariantStream};
use crate::traits::{NextSegmentResult, SegmentType};
use crate::{HlsError, HlsResult, MediaStream};
use std::time::{Duration, Instant};
use tracing::debug;

mod bandwidth_estimator;
mod ewma;

// Selection mode is now modeled as `manual_variant_id: Option<VariantId>` in AbrController.

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
    /// Minimum buffer (in seconds) above which the controller allows up-switching.
    pub min_buffer_for_up_switch: f32,
    /// Buffer (in seconds) below which the controller will be aggressive in down-switching.
    pub down_switch_buffer: f32,
    /// Safety factor applied to throughput when selecting a variant.
    /// For example, if set to 0.8, and throughput is 5 Mbps, ABR will
    /// try to select a variant with bandwidth <= 4 Mbps.
    pub throughput_safety_factor: f32,
    /// Hysteresis ratio applied for up-switch decisions (e.g., 0.15 = +15% headroom).
    pub up_hysteresis_ratio: f32,
    /// Hysteresis ratio applied for down-switch decisions (e.g., 0.05 = -5% margin).
    pub down_hysteresis_ratio: f32,
    /// Minimal interval between consecutive switches to avoid oscillations.
    pub min_switch_interval: Duration,
}

impl Default for AbrConfig {
    fn default() -> Self {
        Self {
            min_buffer_for_up_switch: 0.0,
            down_switch_buffer: 3.0,
            throughput_safety_factor: 0.8,
            up_hysteresis_ratio: 0.15,
            down_hysteresis_ratio: 0.05,
            min_switch_interval: Duration::from_secs(4),
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
    bandwidth_estimator: BandwidthEstimator,
    /// The variant that the controller is currently targeting.
    current_variant_id: Option<VariantId>,
    initial_variant_index: usize,
    /// When set, controller is locked to a specific variant (manual mode).
    manual_variant_id: Option<VariantId>,
    /// Simple wall-clock based buffer estimate (seconds).
    buffer_seconds_estimate: f32,
    /// Last time the buffer estimate was updated.
    last_buffer_update: Instant,
    /// Backoff timer to avoid too frequent switches.
    last_switch_instant: Option<Instant>,
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
    /// * `initial_bandwidth`: The initial bandwidth estimate to use.
    pub fn new(
        stream: S,
        config: AbrConfig,
        manual_variant_id: Option<VariantId>,
        initial_variant_index: usize,
        initial_bandwidth: f64,
    ) -> Self {
        Self {
            stream,
            config,
            bandwidth_estimator: BandwidthEstimator::new(initial_bandwidth),
            current_variant_id: None,
            initial_variant_index,
            manual_variant_id,
            buffer_seconds_estimate: 0.0,
            last_buffer_update: Instant::now(),
            last_switch_instant: None,
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

    /// Consume the controller and return the underlying `MediaStream`.
    pub fn into_inner(self) -> S {
        self.stream
    }

    /// Get the ID of the variant currently targeted by the controller.
    pub fn current_variant_id(&self) -> Option<VariantId> {
        self.current_variant_id
    }

    /// Resets the bandwidth estimator, clearing all historical data.
    ///
    /// This is useful after a network change or a long pause.
    pub fn reset(&mut self) {
        self.bandwidth_estimator.reset();
    }

    /// Get the manual selection target if set (manual mode).
    pub fn manual_selection(&self) -> Option<VariantId> {
        self.manual_variant_id
    }

    /// Switch to AUTO mode (ABR).
    pub fn set_auto(&mut self) {
        self.manual_variant_id = None;
    }

    /// Switch to MANUAL mode and lock to the given variant.
    pub async fn set_manual(&mut self, variant_id: VariantId) -> HlsResult<()> {
        self.manual_variant_id = Some(variant_id);
        self.current_variant_id = Some(variant_id);
        self.stream.select_variant(variant_id).await
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
    pub async fn maybe_switch(&mut self, metrics: &PlaybackMetrics) -> HlsResult<()> {
        // If manual mode is active, ensure we are locked to the manual target and skip ABR.
        if let Some(target) = self.manual_variant_id {
            if Some(target) != self.current_variant_id {
                self.stream.select_variant(target).await?;
                self.current_variant_id = Some(target);
            }
            return Ok(());
        }

        let variants = self.stream.variants();
        if variants.len() <= 1 {
            return Ok(());
        }

        // Current variant bandwidth (bps) if known
        let current_bw = self
            .current_variant_id
            .and_then(|id| variants.iter().find(|v| v.id == id))
            .and_then(|v| v.bandwidth)
            .unwrap_or(0) as f64;

        // Estimated available throughput after applying safety factor
        let estimated_bandwidth = self.bandwidth_estimator.get_estimate();
        let adjusted_bandwidth = estimated_bandwidth * self.config.throughput_safety_factor as f64;

        // Choose the best candidate under adjusted throughput; if none, choose the lowest bandwidth.
        let (candidate_id, candidate_bw) = {
            let best_under: Option<&VariantStream> = variants
                .iter()
                .filter(|v| v.bandwidth.unwrap_or(0) as f64 <= adjusted_bandwidth)
                .max_by_key(|v| v.bandwidth);

            if let Some(v) = best_under {
                (Some(v.id), v.bandwidth.unwrap_or(0) as f64)
            } else {
                let min_v = variants.iter().min_by_key(|v| v.bandwidth);
                if let Some(v) = min_v {
                    (Some(v.id), v.bandwidth.unwrap_or(0) as f64)
                } else {
                    (None, 0.0)
                }
            }
        };

        debug!(
            "ABR: current_bw={:.0}bps, est={:.0}bps, adj={:.0}bps, buffer={:.2}s, candidate_bw={:.0}bps, candidate_id={:?}",
            current_bw,
            estimated_bandwidth,
            adjusted_bandwidth,
            metrics.buffer_seconds,
            candidate_bw,
            candidate_id
        );

        // Backoff to avoid oscillations
        let now = Instant::now();
        let can_switch_time = self
            .last_switch_instant
            .map(|t| now.duration_since(t) >= self.config.min_switch_interval)
            .unwrap_or(true);

        // Buffer driven down-switch urgency
        let urgent_down = metrics.buffer_seconds < self.config.down_switch_buffer;

        if let Some(new_id) = candidate_id {
            if candidate_bw > current_bw {
                // Consider up-switch only if buffer is healthy (or gating disabled), we have enough headroom,
                // and we are past the switch backoff interval.
                let buffer_ok = self.config.min_buffer_for_up_switch <= 0.0
                    || metrics.buffer_seconds >= self.config.min_buffer_for_up_switch;
                let headroom_ok = adjusted_bandwidth
                    >= current_bw * (1.0 + self.config.up_hysteresis_ratio as f64);
                let up_allowed = buffer_ok && headroom_ok && can_switch_time;

                if up_allowed && Some(new_id) != self.current_variant_id {
                    debug!(
                        "ABR: switching UP {:?} -> {:?} (cur_bw={:.0}, cand_bw={:.0}, adj={:.0}, buffer={:.2})",
                        self.current_variant_id,
                        new_id,
                        current_bw,
                        candidate_bw,
                        adjusted_bandwidth,
                        metrics.buffer_seconds
                    );
                    self.stream.select_variant(new_id).await?;
                    self.current_variant_id = Some(new_id);
                    self.last_switch_instant = Some(now);
                } else {
                    debug!(
                        "ABR: skip UP (buffer_ok={}, headroom_ok={}, can_switch_time={}, same_variant={})",
                        buffer_ok,
                        headroom_ok,
                        can_switch_time,
                        Some(new_id) == self.current_variant_id
                    );
                }
            } else if candidate_bw < current_bw {
                // Consider down-switch if buffer is low (urgent) or bandwidth margin suggests it,
                // respecting backoff unless it's urgent.
                let margin_ok = adjusted_bandwidth
                    <= current_bw * (1.0 - self.config.down_hysteresis_ratio as f64);
                let down_allowed = urgent_down || margin_ok;

                if down_allowed
                    && (can_switch_time || urgent_down)
                    && Some(new_id) != self.current_variant_id
                {
                    debug!(
                        "ABR: switching DOWN {:?} -> {:?} (cur_bw={:.0}, cand_bw={:.0}, adj={:.0}, buffer={:.2}, urgent={})",
                        self.current_variant_id,
                        new_id,
                        current_bw,
                        candidate_bw,
                        adjusted_bandwidth,
                        metrics.buffer_seconds,
                        urgent_down
                    );
                    self.stream.select_variant(new_id).await?;
                    self.current_variant_id = Some(new_id);
                    self.last_switch_instant = Some(now);
                } else {
                    debug!(
                        "ABR: skip DOWN (margin_ok={}, urgent_down={}, can_switch_time={}, same_variant={})",
                        margin_ok,
                        urgent_down,
                        can_switch_time,
                        Some(new_id) == self.current_variant_id
                    );
                }
            } // else equal bandwidth: do nothing
        }

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
        // Treat direct select_variant calls as manual override, as in typical players.
        self.manual_variant_id = Some(variant_id);
        self.current_variant_id = Some(variant_id);
        self.stream.select_variant(variant_id).await
    }

    async fn next_segment(&mut self) -> HlsResult<Option<SegmentType>> {
        // Blocking version: loop until we get a segment or end of stream
        loop {
            match self.next_segment_nonblocking().await? {
                NextSegmentResult::Segment(seg) => return Ok(Some(seg)),
                NextSegmentResult::EndOfStream => return Ok(None),
                NextSegmentResult::NeedsRefresh { wait } => {
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }

    async fn next_segment_nonblocking(&mut self) -> HlsResult<NextSegmentResult> {
        // Update simple buffer estimate by accounting for wall-clock elapsed time
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_buffer_update).as_secs_f32();
        if elapsed > 0.0 {
            self.buffer_seconds_estimate = (self.buffer_seconds_estimate - elapsed).max(0.0);
            self.last_buffer_update = now;
        }

        // Build metrics for ABR
        let metrics = PlaybackMetrics {
            throughput_bps: self.bandwidth_estimator.get_estimate() as u64,
            buffer_seconds: self.buffer_seconds_estimate,
            dropped_frames: 0,
        };

        // 1. Run the ABR logic to decide if we should switch streams.
        self.maybe_switch(&metrics).await?;

        // 2. Fetch the next segment using non-blocking method, measuring the time it takes.
        let start_time = Instant::now();
        let result = self.stream.next_segment_nonblocking().await?;
        let duration = start_time.elapsed();

        match &result {
            NextSegmentResult::Segment(segment_type) => {
                match segment_type {
                    SegmentType::Init(_) => {
                        // For init segments, we don't update bandwidth estimator or buffer
                        // because they don't contain media data
                    }
                    SegmentType::Media(segment_data) => {
                        // 3. Feed the measurement to the bandwidth estimator.
                        self.bandwidth_estimator.add_sample(
                            duration.as_millis() as f64,
                            segment_data.data.len() as u32,
                        );

                        // 4. Increase buffer estimate by the segment duration.
                        self.buffer_seconds_estimate += segment_data.duration.as_secs_f32();
                        self.last_buffer_update = Instant::now();
                    }
                }
            }
            NextSegmentResult::EndOfStream | NextSegmentResult::NeedsRefresh { .. } => {
                // No segment to process
            }
        }

        Ok(result)
    }
}
