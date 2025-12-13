//! Thin wrapper around the `stream-download` crate.
//!
//! This module provides a small, HLS-oriented API for downloading arbitrary
//! HTTP resources (playlists, segments) using the `stream-download` crate as
//! the underlying engine.
//!
//! # Performance Optimizations
//!
//! - Uses `BytesMut` for zero-copy conversion to `Bytes`
//! - Configurable buffer sizes for different resource types
//! - Efficient error handling with minimal allocations

use std::io::Read;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
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
/// - global cache configuration overrides;
/// - connection limits / timeouts;
#[derive(Debug, Clone)]
pub struct DownloaderConfig {
    /// Optional default TTL for downloaded resources.
    ///
    /// Semantics of this field are not defined yet and will depend on
    /// how we integrate with `stream-download`'s caching model.
    pub default_ttl: Option<Duration>,
    /// Initial buffer size for playlist downloads (default: 16KB).
    pub playlist_buffer_size: usize,
    /// Initial buffer size for segment downloads (default: 128KB).
    pub segment_buffer_size: usize,
    /// Chunk size for reading from stream (default: 64KB).
    pub read_chunk_size: usize,
}

impl DownloaderConfig {
    /// Create a new configuration optimized for HLS streaming.
    pub fn optimized() -> Self {
        Self {
            default_ttl: None,
            playlist_buffer_size: 16 * 1024, // 16KB for playlists
            segment_buffer_size: 128 * 1024, // 128KB for segments
            read_chunk_size: 64 * 1024,      // 64KB chunks
        }
    }
}

impl Default for DownloaderConfig {
    fn default() -> Self {
        Self::optimized()
    }
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

    /// Create a new `ResourceDownloader` with optimized settings for HLS.
    pub fn optimized() -> Self {
        Self::new(DownloaderConfig::optimized())
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
        self.download_bytes_with_hint(url, _ttl, None).await
    }

    /// Download the resource at the given URL into memory as `Bytes`,
    /// with a hint about the expected resource type for buffer sizing.
    ///
    /// # Arguments
    /// * `url` - The URL to download
    /// * `ttl` - Optional TTL for caching
    /// * `is_segment` - Optional hint: `true` for media segments, `false` for playlists
    pub async fn download_bytes_with_hint(
        &self,
        url: &str,
        _ttl: Option<Duration>,
        is_segment: Option<bool>,
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

        // Choose buffer size based on resource type hint
        let initial_buffer_size = match is_segment {
            Some(true) => self.config.segment_buffer_size,
            Some(false) => self.config.playlist_buffer_size,
            None => self.config.segment_buffer_size, // Default to segment size
        };

        let read_chunk_size = self.config.read_chunk_size;
        let buffer = tokio::task::spawn_blocking(move || {
            let mut r = reader;
            let mut buf = BytesMut::with_capacity(initial_buffer_size);
            let mut chunk = vec![0u8; read_chunk_size];

            loop {
                match r.read(&mut chunk) {
                    Ok(0) => break Ok(buf.freeze()), // Zero-copy conversion to Bytes
                    Ok(n) => {
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    Err(e) => break Err(HlsError::Io(e.to_string())),
                }
            }
        })
        .await
        .map_err(|e| HlsError::Io(format!("join error: {e}")))??;

        Ok(buffer)
    }

    /// Download a playlist (smaller buffer size).
    pub async fn download_playlist(&self, url: &str) -> HlsResult<Bytes> {
        self.download_bytes_with_hint(url, None, Some(false)).await
    }

    /// Download a media segment (larger buffer size).
    pub async fn download_segment(&self, url: &str) -> HlsResult<Bytes> {
        self.download_bytes_with_hint(url, None, Some(true)).await
    }
}
