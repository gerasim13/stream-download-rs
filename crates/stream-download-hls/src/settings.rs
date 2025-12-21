//! Unified configuration for the `stream-download-hls` crate.
//!
//! This structure flattens all previously separate configuration structs
//! into a single type, eliminating the need for `DownloaderConfig`,
//! `HlsConfig`, `AbrConfig`, and `SelectionMode` across the crate.
//!
//! Included configuration domains:
//! - HTTP downloader behavior (timeouts, retries, backoff)
//! - Core HLS behavior (live refresh, retry timeout, key handling)
//! - Adaptive Bitrate (ABR) behavior (hysteresis, safety, buffer thresholds)
//! - Variant selection mode (manual vs. auto)
//!
//! Notes:
//! - Manual selection is represented by `variant_stream_selector` callback returning `Some(id)`.
//!   When it returns `None`, selection is AUTO (ABR-controlled).

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use url::Url;

use crate::model::{KeyProcessorCallback, VariantId};

/// Type alias for variant stream selector callback.
///
/// - Return `Some(VariantId)` to force MANUAL selection for that variant.
/// - Return `None` to allow AUTO selection (ABR-controlled).
///
/// The callback receives the parsed master playlist so you can make an
/// informed choice (bandwidth, codecs, resolution, etc.).
pub type VariantStreamSelector =
    dyn Fn(&crate::model::MasterPlaylist) -> Option<VariantId> + Send + Sync;

/// Unified settings for HLS streaming.
#[derive(Clone)]
pub struct HlsSettings {
    // ----------------------------
    // URL resolution
    // ----------------------------
    /// Optional base URL override used to form final URLs for:
    /// - media playlists (variant URIs in master playlist)
    /// - segments
    /// - encryption keys
    ///
    /// When `None`, URL resolution falls back to the relevant playlist URL.
    pub base_url: Option<Url>,

    // ----------------------------
    // Variant selection
    // ----------------------------
    /// Variant selector callback.
    ///
    /// If it returns `None`, selection is AUTO (ABR-controlled).
    /// If it returns `Some(id)`, selection is MANUAL for that variant.
    ///
    /// The callback is given the parsed master playlist.
    pub variant_stream_selector: Option<Arc<Box<VariantStreamSelector>>>,

    // ----------------------------
    // HTTP downloader (flattened from `DownloaderConfig`)
    // ----------------------------
    /// Timeout for a single HTTP operation (e.g., creating the stream or collecting bytes).
    /// Default: 30 seconds.
    pub request_timeout: Duration,

    /// Maximum number of retry attempts for failed requests.
    /// Default: 3 retries.
    pub max_retries: u32,

    /// Base delay for exponential backoff between retries.
    /// Default: 100ms.
    pub retry_base_delay: Duration,

    /// Maximum backoff delay (cap for exponential growth).
    /// Default: 5 seconds.
    pub max_retry_delay: Duration,

    // ----------------------------
    // Core HLS behavior (flattened from `HlsConfig`)
    // ----------------------------
    /// Optional override for how often live playlists should be refreshed.
    /// If not set, `#EXT-X-TARGETDURATION` should be used.
    pub live_refresh_interval: Option<Duration>,

    /// Timeout for retrying stream operations when no new data is available.
    /// Default: 5 seconds.
    pub retry_timeout: Duration,

    /// Number of segments to prefetch in the buffer.
    /// Larger values can improve smoothness at the cost of memory usage.
    /// Default: 2.
    pub prefetch_buffer_size: usize,

    /// Optional callback to post-process fetched AES keys before use (e.g., unwrap DRM).
    ///
    /// Intentionally boxed and wrapped in `Arc` for cheap clones across tasks.
    /// Not included in Debug output for readability.
    #[cfg(feature = "aes-decrypt")]
    pub key_processor_cb: Option<Arc<Box<KeyProcessorCallback>>>,

    /// Optional query parameters appended to key fetch requests.
    #[cfg(feature = "aes-decrypt")]
    pub key_query_params: Option<HashMap<String, String>>,

    /// Optional headers added to key fetch requests.
    #[cfg(feature = "aes-decrypt")]
    pub key_request_headers: Option<HashMap<String, String>>,

    // ----------------------------
    // ABR behavior (flattened from `AbrConfig`)
    // ----------------------------
    /// Minimum buffer (in seconds) above which the controller allows up-switching.
    /// Default: 0.0 (disabled gating).
    pub abr_min_buffer_for_up_switch: f32,

    /// Buffer (in seconds) below which the controller will be aggressive in down-switching.
    /// Default: 3.0 seconds.
    pub abr_down_switch_buffer: f32,

    /// Safety factor applied to throughput when selecting a variant.
    /// For example, if 0.8 and throughput is 5 Mbps, ABR targets <= 4 Mbps variants.
    /// Default: 0.8.
    pub abr_throughput_safety_factor: f32,

    /// Hysteresis ratio for up-switch decisions (e.g., 0.15 = +15% headroom).
    /// Default: 0.15.
    pub abr_up_hysteresis_ratio: f32,

    /// Hysteresis ratio for down-switch decisions (e.g., 0.05 = -5% margin).
    /// Default: 0.05.
    pub abr_down_hysteresis_ratio: f32,

    /// Minimal interval between consecutive switches to avoid oscillations.
    /// Default: 4 seconds.
    pub abr_min_switch_interval: Duration,
}

impl Default for HlsSettings {
    fn default() -> Self {
        Self {
            // URL resolution defaults
            base_url: None,

            // Variant selection defaults
            variant_stream_selector: None,

            // Downloader defaults (from previous `DownloaderConfig::default`)
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_base_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(5),

            // HLS defaults (from previous `HlsConfig::default`)
            live_refresh_interval: None,
            retry_timeout: Duration::from_secs(5),
            key_processor_cb: None,
            key_query_params: None,
            key_request_headers: None,
            prefetch_buffer_size: 2,

            // ABR defaults (from previous `AbrConfig::default`)
            abr_min_buffer_for_up_switch: 0.0,
            abr_down_switch_buffer: 3.0,
            abr_throughput_safety_factor: 0.8,
            abr_up_hysteresis_ratio: 0.15,
            abr_down_hysteresis_ratio: 0.05,
            abr_min_switch_interval: Duration::from_secs(4),
        }
    }
}

impl fmt::Debug for HlsSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Do not print `key_processor_cb` to keep Debug output clean.
        f.debug_struct("HlsSettings")
            // URL resolution + Selection
            .field("base_url", &self.base_url)
            .field("variant_stream_selector", &"<callback>")
            // Downloader
            .field("request_timeout", &self.request_timeout)
            .field("max_retries", &self.max_retries)
            .field("retry_base_delay", &self.retry_base_delay)
            .field("max_retry_delay", &self.max_retry_delay)
            // HLS
            .field("live_refresh_interval", &self.live_refresh_interval)
            .field("retry_timeout", &self.retry_timeout)
            .field("key_query_params", &self.key_query_params)
            .field("key_request_headers", &self.key_request_headers)
            .field("prefetch_buffer_size", &self.prefetch_buffer_size)
            // ABR
            .field(
                "abr_min_buffer_for_up_switch",
                &self.abr_min_buffer_for_up_switch,
            )
            .field("abr_down_switch_buffer", &self.abr_down_switch_buffer)
            .field(
                "abr_throughput_safety_factor",
                &self.abr_throughput_safety_factor,
            )
            .field("abr_up_hysteresis_ratio", &self.abr_up_hysteresis_ratio)
            .field("abr_down_hysteresis_ratio", &self.abr_down_hysteresis_ratio)
            .field("abr_min_switch_interval", &self.abr_min_switch_interval)
            .finish()
    }
}

impl HlsSettings {
    // -------------------------
    // Constructors
    // -------------------------

    /// Create default settings.
    pub fn new() -> Self {
        Self::default()
    }

    // -------------------------
    // URL resolution helpers
    // -------------------------

    /// Override base URL used to form final URLs for segments and keys.
    pub fn base_url(mut self, base_url: Url) -> Self {
        self.base_url = Some(base_url);
        self
    }

    /// Clear the base URL override.
    pub fn clear_base_url(mut self) -> Self {
        self.base_url = None;
        self
    }

    // -------------------------
    // Variant selection helpers
    // -------------------------

    /// Select AUTO variant selection (ABR-controlled).
    ///
    /// Equivalent to clearing the selector callback.
    pub fn selection_auto(mut self) -> Self {
        self.variant_stream_selector = None;
        self
    }

    /// Set a variant selector callback.
    ///
    /// - Return `None` to allow AUTO selection (ABR-controlled).
    /// - Return `Some(VariantId)` to force MANUAL selection for that variant.
    pub fn variant_stream_selector(
        mut self,
        cb: impl Fn(&crate::model::MasterPlaylist) -> Option<VariantId> + Send + Sync + 'static,
    ) -> Self {
        self.variant_stream_selector = Some(Arc::new(Box::new(cb)));
        self
    }

    /// Convenience helper for fixed MANUAL selection.
    pub fn selection_manual(mut self, variant_id: VariantId) -> Self {
        self.variant_stream_selector = Some(Arc::new(Box::new(move |_master| Some(variant_id))));
        self
    }

    /// Returns true if selection is AUTO (no selector callback set).
    pub fn is_selection_auto(&self) -> bool {
        self.variant_stream_selector.is_none()
    }

    // -------------------------
    // Downloader setters
    // -------------------------

    pub fn request_timeout(mut self, v: Duration) -> Self {
        self.request_timeout = v;
        self
    }

    pub fn max_retries(mut self, v: u32) -> Self {
        self.max_retries = v;
        self
    }

    pub fn retry_base_delay(mut self, v: Duration) -> Self {
        self.retry_base_delay = v;
        self
    }

    pub fn max_retry_delay(mut self, v: Duration) -> Self {
        self.max_retry_delay = v;
        self
    }

    // -------------------------
    // HLS setters
    // -------------------------

    pub fn live_refresh_interval(mut self, v: Option<Duration>) -> Self {
        self.live_refresh_interval = v;
        self
    }

    pub fn retry_timeout(mut self, v: Duration) -> Self {
        self.retry_timeout = v;
        self
    }

    pub fn key_processor_cb(mut self, cb: Option<Arc<Box<KeyProcessorCallback>>>) -> Self {
        self.key_processor_cb = cb;
        self
    }

    pub fn key_query_params(mut self, params: Option<HashMap<String, String>>) -> Self {
        self.key_query_params = params;
        self
    }

    pub fn key_request_headers(mut self, headers: Option<HashMap<String, String>>) -> Self {
        self.key_request_headers = headers;
        self
    }

    pub fn prefetch_buffer_size(mut self, v: usize) -> Self {
        self.prefetch_buffer_size = v;
        self
    }

    // -------------------------
    // ABR setters
    // -------------------------

    pub fn abr_min_buffer_for_up_switch(mut self, v: f32) -> Self {
        self.abr_min_buffer_for_up_switch = v;
        self
    }

    pub fn abr_down_switch_buffer(mut self, v: f32) -> Self {
        self.abr_down_switch_buffer = v;
        self
    }

    pub fn abr_throughput_safety_factor(mut self, v: f32) -> Self {
        self.abr_throughput_safety_factor = v;
        self
    }

    pub fn abr_up_hysteresis_ratio(mut self, v: f32) -> Self {
        self.abr_up_hysteresis_ratio = v;
        self
    }

    pub fn abr_down_hysteresis_ratio(mut self, v: f32) -> Self {
        self.abr_down_hysteresis_ratio = v;
        self
    }

    pub fn abr_min_switch_interval(mut self, v: Duration) -> Self {
        self.abr_min_switch_interval = v;
        self
    }
}
