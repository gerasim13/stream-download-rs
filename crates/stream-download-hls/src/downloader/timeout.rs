//! Timeout decorator for downloaders.

use std::time::Duration;

use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::error::{HlsError, HlsResult};

use super::traits::{ByteStream, Downloader, Headers};

/// Downloader decorator that adds timeout enforcement.
#[derive(Debug, Clone)]
pub struct TimeoutDownloader<D> {
    inner: D,
    request_timeout: Duration,
}

impl<D> TimeoutDownloader<D> {
    /// Create a new timeout decorator.
    pub fn new(inner: D, request_timeout: Duration) -> Self {
        Self {
            inner,
            request_timeout,
        }
    }

    /// Apply timeout to a future.
    async fn with_timeout<T, F>(&self, fut: F, url: &str) -> HlsResult<T>
    where
        F: std::future::Future<Output = HlsResult<T>>,
    {
        match timeout(self.request_timeout, fut).await {
            Ok(result) => result,
            Err(_) => Err(HlsError::timeout(url.to_string())),
        }
    }
}

#[async_trait::async_trait]
impl<D> Downloader for TimeoutDownloader<D>
where
    D: Downloader + Send + Sync,
{
    async fn download(&self, url: &str) -> HlsResult<bytes::Bytes> {
        self.with_timeout(self.inner.download(url), url).await
    }

    async fn download_with_headers(
        &self,
        url: &str,
        headers: Option<Headers>,
    ) -> HlsResult<bytes::Bytes> {
        self.with_timeout(self.inner.download_with_headers(url, headers), url)
            .await
    }

    async fn stream(&self, url: &str) -> HlsResult<ByteStream> {
        // For streaming operations, we apply timeout to the stream creation
        // but not to the stream itself (it's long-lived).
        self.with_timeout(self.inner.stream(url), url).await
    }

    async fn stream_range(&self, url: &str, start: u64, end: Option<u64>) -> HlsResult<ByteStream> {
        self.with_timeout(self.inner.stream_range(url, start, end), url)
            .await
    }

    async fn probe_content_length(&self, url: &str) -> HlsResult<Option<u64>> {
        self.with_timeout(self.inner.probe_content_length(url), url)
            .await
    }

    fn cancel_token(&self) -> &CancellationToken {
        self.inner.cancel_token()
    }
}
