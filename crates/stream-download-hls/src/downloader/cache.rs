//! Cache decorator for downloaders.

use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tracing::trace;

use stream_download::source::ResourceKey;
use stream_download::storage::StorageHandle;

use crate::error::{HlsError, HlsResult};

use super::traits::{ByteStream, Downloader, Headers};

/// Where returned bytes came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheSource {
    /// Read from cache.
    Cache,
    /// Downloaded from network.
    Network,
}

/// Bytes returned by a cached download.
#[derive(Clone, Debug)]
pub struct CachedBytes {
    /// The downloaded bytes.
    pub bytes: Bytes,
    /// Source of the bytes.
    pub source: CacheSource,
}

/// Downloader decorator that adds caching.
#[derive(Debug, Clone)]
pub struct CacheDownloader<D> {
    inner: D,
    handle: Option<StorageHandle>,
    /// If true, cache read errors are treated as misses.
    best_effort_cache: bool,
}

impl<D> CacheDownloader<D>
where
    D: Downloader + Send + Sync,
{
    /// Create a new cache decorator.
    pub fn new(inner: D, handle: Option<StorageHandle>) -> Self {
        Self {
            inner,
            handle,
            best_effort_cache: true,
        }
    }

    /// Create a cache decorator with caching disabled.
    pub fn new_uncached(inner: D) -> Self {
        Self::new(inner, None)
    }

    /// Set whether cache read errors are treated as misses.
    pub fn with_best_effort_cache(mut self, enabled: bool) -> Self {
        self.best_effort_cache = enabled;
        self
    }

    /// Replace the storage handle.
    pub fn with_storage_handle(mut self, handle: Option<StorageHandle>) -> Self {
        self.handle = handle;
        self
    }

    /// Read from cache.
    fn read_cache(&self, key: &ResourceKey) -> HlsResult<Option<Bytes>> {
        let Some(handle) = &self.handle else {
            trace!("cache: disabled; key='{}'", key.0);
            return Ok(None);
        };

        match handle.read(key) {
            Ok(Some(bytes)) => {
                trace!("cache: HIT key='{}' ({} bytes)", key.0, bytes.len());
                Ok(Some(bytes))
            }
            Ok(None) => {
                trace!("cache: MISS key='{}'", key.0);
                Ok(None)
            }
            Err(e) => {
                trace!("cache: READ ERROR key='{}' err='{}'", key.0, e);
                if self.best_effort_cache {
                    trace!("cache: treating read error as miss (best_effort_cache=true)");
                    Ok(None)
                } else {
                    Err(HlsError::io(format!(
                        "cache read failed for key '{}': {e}",
                        key.0
                    )))
                }
            }
        }
    }

    /// Download with caching using a resource key.
    pub async fn download_cached(&self, url: &str, key: &ResourceKey) -> HlsResult<CachedBytes> {
        trace!("cache: request url='{}' key='{}'", url, key.0);
        if let Some(bytes) = self.read_cache(key)? {
            trace!("cache: serving from cache key='{}'", key.0);
            return Ok(CachedBytes {
                bytes,
                source: CacheSource::Cache,
            });
        }

        trace!(
            "cache: downloading from network url='{}' key='{}'",
            url, key.0
        );
        let bytes = self.inner.download(url).await?;
        trace!(
            "cache: downloaded from network url='{}' key='{}' ({} bytes)",
            url,
            key.0,
            bytes.len()
        );
        Ok(CachedBytes {
            bytes,
            source: CacheSource::Network,
        })
    }

    /// Download with caching using a resource key and custom headers.
    pub async fn download_cached_with_headers(
        &self,
        url: &str,
        key: &ResourceKey,
        headers: Option<Headers>,
    ) -> HlsResult<CachedBytes> {
        trace!("cache: request with headers url='{}' key='{}'", url, key.0);
        if let Some(bytes) = self.read_cache(key)? {
            trace!("cache: serving from cache key='{}'", key.0);
            return Ok(CachedBytes {
                bytes,
                source: CacheSource::Cache,
            });
        }

        trace!(
            "cache: downloading from network with headers url='{}' key='{}'",
            url, key.0
        );
        let bytes = self.inner.download_with_headers(url, headers).await?;
        trace!(
            "cache: downloaded from network url='{}' key='{}' ({} bytes)",
            url,
            key.0,
            bytes.len()
        );
        Ok(CachedBytes {
            bytes,
            source: CacheSource::Network,
        })
    }
}

#[async_trait::async_trait]
impl<D> Downloader for CacheDownloader<D>
where
    D: Downloader + Send + Sync,
{
    async fn download(&self, url: &str) -> HlsResult<Bytes> {
        // Without a resource key, we can't cache - just pass through
        self.inner.download(url).await
    }

    async fn download_with_headers(&self, url: &str, headers: Option<Headers>) -> HlsResult<Bytes> {
        // Without a resource key, we can't cache - just pass through
        self.inner.download_with_headers(url, headers).await
    }

    async fn stream(&self, url: &str) -> HlsResult<ByteStream> {
        // Streaming doesn't use cache
        self.inner.stream(url).await
    }

    async fn stream_range(&self, url: &str, start: u64, end: Option<u64>) -> HlsResult<ByteStream> {
        // Streaming doesn't use cache
        self.inner.stream_range(url, start, end).await
    }

    async fn probe_content_length(&self, url: &str) -> HlsResult<Option<u64>> {
        // Probing doesn't use cache
        self.inner.probe_content_length(url).await
    }

    fn cancel_token(&self) -> &CancellationToken {
        self.inner.cancel_token()
    }
}
