//! Downloader traits for HLS resources.

use std::collections::HashMap;

use bytes::Bytes;
use futures_util::stream::BoxStream;

use crate::error::{HlsError, HlsResult};

/// A stream of bytes with potential errors.
pub type ByteStream = BoxStream<'static, Result<Bytes, HlsError>>;

/// Headers for HTTP requests.
pub type Headers = HashMap<String, String>;

/// Core trait for downloading resources.
///
/// This trait provides a unified interface for downloading various types of HLS resources
/// (playlists, keys, segments) with optional headers and range requests.
#[async_trait::async_trait]
pub trait Downloader: Send + Sync {
    /// Download bytes from a URL.
    async fn download(&self, url: &str) -> HlsResult<Bytes>;

    /// Download bytes from a URL with custom headers.
    async fn download_with_headers(&self, url: &str, headers: Option<Headers>) -> HlsResult<Bytes>;

    /// Stream bytes from a URL.
    async fn stream(&self, url: &str) -> HlsResult<ByteStream>;

    /// Stream bytes from a URL with a byte range.
    async fn stream_range(&self, url: &str, start: u64, end: Option<u64>) -> HlsResult<ByteStream>;

    /// Probe content length of a resource.
    async fn probe_content_length(&self, url: &str) -> HlsResult<Option<u64>>;

    /// Get cancellation token for this downloader.
    fn cancel_token(&self) -> &tokio_util::sync::CancellationToken;
}

/// Extension methods for Downloader trait.
#[async_trait::async_trait]
pub trait DownloaderExt: Downloader {
    /// Download a playlist (convenience method).
    async fn download_playlist(&self, url: &str) -> HlsResult<Bytes> {
        self.download(url).await
    }

    /// Download an encryption key with optional key-specific headers.
    async fn download_key(&self, url: &str, key_headers: Option<Headers>) -> HlsResult<Bytes> {
        self.download_with_headers(url, key_headers).await
    }

    /// Download bytes with retry logic (convenience for decorators).
    async fn download_with_retry(&self, url: &str) -> HlsResult<Bytes> {
        self.download(url).await
    }
}

// Blanket implementation for all Downloader types
#[async_trait::async_trait]
impl<D: Downloader + ?Sized> DownloaderExt for D {}
