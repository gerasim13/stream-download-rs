/stream-download-rs/crates/stream-download-hls/src/downloader.rs#L1-200
//! Thin wrapper around the `stream-download` crate.
//!
//! This module is intentionally minimal at this stage of the PoC.
//! Its main responsibility is to provide a small, HLS-oriented API
//! for downloading arbitrary HTTP resources (playlists, segments)
//! using the `stream-download` crate as the underlying engine.
//!
//! Design notes:
//! - We keep this layer small so that HLS code in `manager` and `abr`
//!   does not depend directly on `stream-download` internals.
//! - For now we expose a very simple `download_bytes` method that
//!   synchronously downloads a resource into memory. Later we can
//!   extend this with more advanced features (shared clients,
//!   configurable cache policies, streaming reads, etc.).
//!
//! The exact integration with `stream-download` will be implemented
//! in later iterations once the PoC API stabilizes.

use std::time::Duration;

use crate::model::{HlsError, HlsResult};

/// Configuration for the `ResourceDownloader`.
///
/// This is intentionally small for now. In the future it may include
/// things like:
//  - global cache configuration overrides;
//  - connection limits / timeouts;
//  - shared HTTP client handles;
#[derive(Debug, Clone, Default)]
pub struct DownloaderConfig {
    /// Optional default TTL for downloaded resources.
    ///
    /// Semantics of this field are not defined yet and will depend on
    /// how we integrate with `stream-download`'s caching model.
    pub default_ttl: Option<Duration>,
}

/// Thin wrapper over `stream-download` for fetching arbitrary URLs.
///
/// At this PoC stage, this type is mostly a placeholder, but we
/// already define its public surface so that higher-level modules
/// (`manager`, `parser`, `abr`) can depend on it without pulling in
/// `stream-download` types directly.
#[derive(Debug, Clone)]
pub struct ResourceDownloader {
    config: DownloaderConfig,
    // In a future iteration this will likely hold some kind of
    // shared client / handle from `stream-download`.
    //
    // Example (pseudocode):
    //   client: stream_download::Client,
}

impl ResourceDownloader {
    /// Create a new `ResourceDownloader` with the given configuration.
    pub fn new(config: DownloaderConfig) -> Self {
        Self { config }
    }

    /// Access the configuration used by this downloader.
    pub fn config(&self) -> &DownloaderConfig {
        &self.config
    }

    /// Download the resource at the given URL into memory.
    ///
    /// For now this is a stub that always returns an error, so we can
    /// define and use the API in other modules without coupling them
    /// to an unfinished implementation.
    ///
    /// Later this method will:
    /// - Construct an appropriate `stream-download` resource/client
    ///   for the specified URL.
    /// - Apply caching / TTL policies as appropriate.
    /// - Return the loaded bytes as `Vec<u8>`.
    pub async fn download_bytes(
        &self,
        url: &str,
        ttl: Option<Duration>,
    ) -> HlsResult<Vec<u8>> {
        let _ = ttl;
        Err(HlsError::Message(format!(
            "ResourceDownloader::download_bytes is not implemented yet (url={url})"
        )))
    }
}
