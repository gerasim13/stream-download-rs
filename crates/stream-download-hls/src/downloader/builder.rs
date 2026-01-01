//! Builder for composing downloader decorators.

use std::sync::Arc;
use std::time::Duration;

use super::cache::CacheKeyCallback;
use super::traits::{Downloader, Headers};
use super::{CacheDownloader, HttpDownloader, RetryDownloader, RetryPolicy, TimeoutDownloader};
use crate::storage_new::HlsStorageProvider;

/// Builder for creating composed downloaders.
pub struct DownloaderBuilder<D> {
    inner: D,
}

impl DownloaderBuilder<HttpDownloader> {
    /// Start building from a base HttpDownloader.
    pub fn from_http(downloader: HttpDownloader) -> Self {
        Self { inner: downloader }
    }
}

impl<D> DownloaderBuilder<D>
where
    D: Downloader + Send + Sync + Clone + 'static,
{
    /// Add retry functionality.
    pub fn with_retry(self, policy: RetryPolicy) -> DownloaderBuilder<RetryDownloader<D>> {
        DownloaderBuilder {
            inner: RetryDownloader::new(self.inner, policy),
        }
    }

    /// Add retry functionality with default policy.
    pub fn with_default_retry(self) -> DownloaderBuilder<RetryDownloader<D>> {
        DownloaderBuilder {
            inner: RetryDownloader::with_default_policy(self.inner),
        }
    }

    /// Add timeout functionality.
    pub fn with_timeout(
        self,
        request_timeout: Duration,
    ) -> DownloaderBuilder<TimeoutDownloader<D>> {
        DownloaderBuilder {
            inner: TimeoutDownloader::new(self.inner, request_timeout),
        }
    }

    /// Add caching functionality.
    pub fn with_cache(
        self,
        cache_provider: Arc<HlsStorageProvider>,
        key_callback: Arc<CacheKeyCallback>,
    ) -> DownloaderBuilder<CacheDownloader<D>> {
        DownloaderBuilder {
            inner: CacheDownloader::new(self.inner, cache_provider, key_callback),
        }
    }

    /// Build the final downloader.
    pub fn build(self) -> D {
        self.inner
    }
}

/// Convenience function to create a downloader with common defaults.
pub fn create_default_downloader(
    request_timeout: Duration,
    max_retries: u32,
    retry_base_delay: Duration,
    max_retry_delay: Duration,
    cancel: tokio_util::sync::CancellationToken,
    cache_provider: Option<Arc<HlsStorageProvider>>,
    key_callback: Arc<CacheKeyCallback>,
    key_request_headers: Option<Headers>,
) -> Arc<dyn Downloader + Send + Sync> {
    let base_downloader = HttpDownloader::new(request_timeout, cancel, key_request_headers);

    let retry_policy = RetryPolicy {
        max_retries,
        base_delay: retry_base_delay,
        max_delay: max_retry_delay,
    };

    // Start with base downloader
    let downloader_builder = DownloaderBuilder::from_http(base_downloader)
        .with_timeout(request_timeout)
        .with_retry(retry_policy);

    // Add cache if provider is available
    let downloader: Arc<dyn Downloader + Send + Sync> = if let Some(cache_provider) = cache_provider
    {
        Arc::new(
            downloader_builder
                .with_cache(cache_provider, key_callback)
                .build(),
        )
    } else {
        Arc::new(downloader_builder.build())
    };

    downloader
}
