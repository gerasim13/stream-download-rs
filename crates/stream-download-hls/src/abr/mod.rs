//! Adaptive Bitrate (ABR) controller.
//!
//! Adds ABR-oriented variant selection on top of a [`MediaStream`] and delegates switching to the
//! wrapped stream (typically [`crate::HlsManager`]).

use std::time::{Duration, Instant};
use tracing::debug;

use self::bandwidth_estimator::BandwidthEstimator;
use crate::manager::NextSegmentDescResult;
use crate::parser::{VariantId, VariantStream};
use crate::{HlsManager, HlsResult, MediaStream};

mod bandwidth_estimator;
mod ewma;

// Selection mode is now modeled as `manual_variant_id: Option<VariantId>` in AbrController.

/// Playback/network metrics used for ABR decisions.
#[derive(Debug, Clone)]
pub struct PlaybackMetrics {
    /// Estimated available throughput in bits per second.
    pub throughput_bps: u64,
    /// How many seconds of media are currently buffered.
    pub buffer_seconds: f32,
    /// Number of dropped frames or similar quality metric.
    pub dropped_frames: u32,
}

/// Configuration for ABR decisions.
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

/// ABR controller that selects variants on top of a [`MediaStream`].
///
/// This type does not perform network I/O; switching is delegated to the wrapped stream.
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
    /// Creates a new ABR controller.
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

    /// Resets the bandwidth estimator state.
    pub fn reset(&mut self) {
        self.bandwidth_estimator.reset();
    }

    /// Returns the manual selection target (if set).
    pub fn manual_selection(&self) -> Option<VariantId> {
        self.manual_variant_id
    }

    /// Initializes the underlying stream and selects the initial variant.
    pub async fn init(&mut self) -> HlsResult<()> {
        self.stream.init().await?;

        // After init, the variants are available. Let's select the initial one.
        let initial_variant_id = self
            .stream
            .variants()
            .get(self.initial_variant_index)
            .map(|v| v.id);

        if let Some(id) = initial_variant_id {
            self.stream.select_variant(id).await?;
            self.current_variant_id = Some(id);
        }

        Ok(())
    }

    /// Reports a downloaded media segment for throughput/buffer estimation.
    pub fn on_media_segment_downloaded(
        &mut self,
        duration: Duration,
        byte_len: u64,
        elapsed: Duration,
    ) {
        // Feed sample to bandwidth estimator. Use sub-millisecond precision to avoid zero-duration
        // samples that would otherwise produce a zero-weight update.
        let duration_ms = (elapsed.as_secs_f64() * 1000.0).max(0.001);
        self.bandwidth_estimator
            .add_sample(duration_ms, byte_len.min(u32::MAX as u64) as u32);

        // Increase buffer estimate by the segment duration.
        self.buffer_seconds_estimate += duration.as_secs_f32();
        self.last_buffer_update = Instant::now();
    }

    /// Switches to AUTO mode (ABR-controlled).
    pub fn set_auto(&mut self) {
        self.manual_variant_id = None;
    }

    /// Switches to MANUAL mode and locks to the given variant.
    pub async fn set_manual(&mut self, variant_id: VariantId) -> HlsResult<()> {
        self.manual_variant_id = Some(variant_id);
        self.current_variant_id = Some(variant_id);
        self.stream.select_variant(variant_id).await
    }

    /// Potentially switches variants based on `metrics` (no-op in manual mode).
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

        let current_bw = self.get_current_bandwidth(&variants);
        let estimated_bandwidth = self.bandwidth_estimator.get_estimate();
        let adjusted_bandwidth = estimated_bandwidth * self.config.throughput_safety_factor as f64;

        let (candidate_id, candidate_bw) =
            self.select_candidate_variant(&variants, adjusted_bandwidth);
        debug!(
            "ABR: current_bw={:.0}bps, est={:.0}bps, adj={:.0}bps, buffer={:.2}s, candidate_bw={:.0}bps, candidate_id={:?}",
            current_bw,
            estimated_bandwidth,
            adjusted_bandwidth,
            metrics.buffer_seconds,
            candidate_bw,
            candidate_id
        );

        let now = Instant::now();
        let can_switch_time = self.can_switch_now(now);
        let urgent_down = metrics.buffer_seconds < self.config.down_switch_buffer;

        if let Some(new_id) = candidate_id {
            if candidate_bw > current_bw {
                self.handle_up_switch(
                    new_id,
                    current_bw,
                    candidate_bw,
                    adjusted_bandwidth,
                    metrics.buffer_seconds,
                    can_switch_time,
                    now,
                )
                .await?;
            } else if candidate_bw < current_bw {
                self.handle_down_switch(
                    new_id,
                    current_bw,
                    candidate_bw,
                    adjusted_bandwidth,
                    metrics.buffer_seconds,
                    urgent_down,
                    can_switch_time,
                    now,
                )
                .await?;
            }
            // equal bandwidth: do nothing
        }

        Ok(())
    }

    /// Get the bandwidth of the current variant.
    fn get_current_bandwidth(&self, variants: &[VariantStream]) -> f64 {
        self.current_variant_id
            .and_then(|id| variants.iter().find(|v| v.id == id))
            .and_then(|v| v.bandwidth)
            .unwrap_or(0) as f64
    }

    /// Select the best candidate variant based on adjusted bandwidth.
    fn select_candidate_variant(
        &self,
        variants: &[VariantStream],
        adjusted_bandwidth: f64,
    ) -> (Option<VariantId>, f64) {
        // Choose the best candidate under adjusted throughput; if none, choose the lowest bandwidth.
        let best_under = variants
            .iter()
            .filter(|v| v.bandwidth.unwrap_or(0) as f64 <= adjusted_bandwidth)
            .max_by_key(|v| v.bandwidth);

        if let Some(v) = best_under {
            (Some(v.id), v.bandwidth.unwrap_or(0) as f64)
        } else {
            let min_v = variants.iter().min_by_key(|v| v.bandwidth);
            min_v
                .map(|v| (Some(v.id), v.bandwidth.unwrap_or(0) as f64))
                .unwrap_or((None, 0.0))
        }
    }

    /// Check if enough time has passed since the last switch.
    fn can_switch_now(&self, now: Instant) -> bool {
        self.last_switch_instant
            .map(|t| now.duration_since(t) >= self.config.min_switch_interval)
            .unwrap_or(true)
    }

    /// Handle up-switch decision and execution.
    async fn handle_up_switch(
        &mut self,
        new_id: VariantId,
        current_bw: f64,
        candidate_bw: f64,
        adjusted_bandwidth: f64,
        buffer_seconds: f32,
        can_switch_time: bool,
        now: Instant,
    ) -> HlsResult<()> {
        // Consider up-switch only if buffer is healthy (or gating disabled), we have enough headroom,
        // and we are past the switch backoff interval.
        let buffer_ok = self.config.min_buffer_for_up_switch <= 0.0
            || buffer_seconds >= self.config.min_buffer_for_up_switch;
        let headroom_ok =
            adjusted_bandwidth >= current_bw * (1.0 + self.config.up_hysteresis_ratio as f64);
        let up_allowed = buffer_ok && headroom_ok && can_switch_time;

        if up_allowed && Some(new_id) != self.current_variant_id {
            debug!(
                "ABR: switching UP {:?} -> {:?} (cur_bw={:.0}, cand_bw={:.0}, adj={:.0}, buffer={:.2})",
                self.current_variant_id,
                new_id,
                current_bw,
                candidate_bw,
                adjusted_bandwidth,
                buffer_seconds
            );
            self.perform_switch(new_id, now).await?;
        } else {
            debug!(
                "ABR: skip UP (buffer_ok={}, headroom_ok={}, can_switch_time={}, same_variant={})",
                buffer_ok,
                headroom_ok,
                can_switch_time,
                Some(new_id) == self.current_variant_id
            );
        }
        Ok(())
    }

    /// Handle down-switch decision and execution.
    async fn handle_down_switch(
        &mut self,
        new_id: VariantId,
        current_bw: f64,
        candidate_bw: f64,
        adjusted_bandwidth: f64,
        buffer_seconds: f32,
        urgent_down: bool,
        can_switch_time: bool,
        now: Instant,
    ) -> HlsResult<()> {
        // Consider down-switch if buffer is low (urgent) or bandwidth margin suggests it,
        // respecting backoff unless it's urgent.
        let margin_ok =
            adjusted_bandwidth <= current_bw * (1.0 - self.config.down_hysteresis_ratio as f64);
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
                buffer_seconds,
                urgent_down
            );
            self.perform_switch(new_id, now).await?;
        } else {
            debug!(
                "ABR: skip DOWN (margin_ok={}, urgent_down={}, can_switch_time={}, same_variant={})",
                margin_ok,
                urgent_down,
                can_switch_time,
                Some(new_id) == self.current_variant_id
            );
        }
        Ok(())
    }

    /// Perform the actual variant switch.
    async fn perform_switch(&mut self, new_id: VariantId, now: Instant) -> HlsResult<()> {
        self.stream.select_variant(new_id).await?;
        self.current_variant_id = Some(new_id);
        self.last_switch_instant = Some(now);
        Ok(())
    }
}

impl AbrController<HlsManager> {
    /// Returns the next segment descriptor with ABR switching (specialized for `HlsManager`).
    pub async fn next_segment_descriptor_nonblocking(
        &mut self,
    ) -> HlsResult<NextSegmentDescResult> {
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

        // Run ABR switching logic before asking the manager for the next descriptor.
        self.maybe_switch(&metrics).await?;
        self.stream.next_segment_descriptor_nonblocking().await
    }
}
