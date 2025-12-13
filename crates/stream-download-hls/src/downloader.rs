//! Thin wrapper around the `stream-download` crate.
//!
//! This module provides a small, HLS-oriented API for downloading arbitrary
//! HTTP resources (playlists, segments) using the `stream-download` crate as
//! the underlying engine.

use std::io::Read;
use std::time::Duration;

use bytes::Bytes;
use reqwest::Url;
use stream_download::Settings;
use stream_download::StreamDownload;
use stream_download::source::DecodeError;
use stream_download::storage::temp::TempStorageProvider;

use crate::model::{HlsError, HlsResult};

/// Configuration for the `ResourceDownloader`.
///
/// This is intentionally small for now. In the future it may include
/// things like:
//  - global cache configuration overrides;
//  - connection limits / timeouts;
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
/// This downloader holds a `stream_download::Client` to reuse connections
/// and (potentially) a storage backend for caching.
#[derive(Debug, Clone)]
pub struct ResourceDownloader {
    config: DownloaderConfig,
}

impl ResourceDownloader {
    /// Create a new `ResourceDownloader` with the given configuration.
    ///
    /// For now, this uses default `stream_download` settings and a temporary storage provider.
    pub fn new(config: DownloaderConfig) -> Self {
        Self { config }
    }

    /// Access the configuration used by this downloader.
    pub fn config(&self) -> &DownloaderConfig {
        &self.config
    }

    /// Download the resource at the given URL into memory as `Bytes`.
    ///
    /// This method leverages `stream-download` to fetch the content.
    pub async fn download_bytes(
        &self,
        url: &str,
        _ttl: Option<Duration>, // ttl is unused for now
    ) -> HlsResult<Bytes> {
        let url = Url::parse(url).map_err(|e| HlsError::Io(e.to_string()))?;
        let reader = match StreamDownload::new_http(
            url,
            TempStorageProvider::default(),
            Settings::default(),
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                let msg = e.decode_error().await;
                return Err(HlsError::Io(msg));
            }
        };

        let buffer = tokio::task::spawn_blocking(move || {
            let mut r = reader;
            let mut buf = Vec::with_capacity(128 * 1024);
            let mut chunk = [0u8; 64 * 1024];
            loop {
                match r.read(&mut chunk) {
                    Ok(0) => break Ok(buf),
                    Ok(n) => {
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    Err(e) => break Err(HlsError::Io(e.to_string())),
                }
            }
        })
        .await
        .map_err(|e| HlsError::Io(format!("join error: {e}")))??;

        Ok(Bytes::from(buffer))
    }
}
