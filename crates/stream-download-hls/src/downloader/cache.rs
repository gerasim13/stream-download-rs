//! Cache decorator for downloaders.

use bytes::Bytes;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::trace;

use super::traits::{ByteStream, Downloader, Headers};
use super::types::Resource;
use crate::cache::keys::HlsCacheKey;
use crate::error::{HlsError, HlsResult};
use crate::storage_new::HlsStorageProvider;

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

/// Callback function type for generating cache keys from resources.
pub type CacheKeyCallback = dyn Fn(&Resource) -> Option<HlsCacheKey> + Send + Sync;

/// Downloader decorator that adds caching.
pub struct CacheDownloader<D> {
    inner: D,
    cache_provider: Arc<HlsStorageProvider>,
    /// Callback to generate cache key from resource
    key_callback: Arc<CacheKeyCallback>,
}

impl<D> CacheDownloader<D>
where
    D: Downloader + Send + Sync,
{
    /// Create a new cache decorator.
    pub fn new(
        inner: D,
        cache_provider: Arc<HlsStorageProvider>,
        key_callback: Arc<CacheKeyCallback>,
    ) -> Self {
        Self {
            inner,
            cache_provider,
            key_callback,
        }
    }

    /// Replace the cache provider.
    pub fn with_cache_provider(mut self, cache_provider: Arc<HlsStorageProvider>) -> Self {
        self.cache_provider = cache_provider;
        self
    }

    /// Set the cache key callback.
    pub fn with_key_callback(mut self, key_callback: Arc<CacheKeyCallback>) -> Self {
        self.key_callback = key_callback;
        self
    }

    /// Read from cache.
    fn read_cache(&self, key: &HlsCacheKey) -> HlsResult<Option<Bytes>> {
        match self.cache_provider.get(key) {
            Ok(Some(bytes)) => Ok(Some(bytes)),
            Err(e) => {
                trace!("cache: read error for key='{}': {:?}", key.as_str(), e);
                Ok(None)
            }
            Ok(None) => Ok(None),
        }
    }

    /// Write to cache.
    fn write_cache(&self, key: &HlsCacheKey, data: Bytes) -> HlsResult<()> {
        self.cache_provider.put(key, data).map_err(|e| {
            HlsError::io(format!(
                "cache: write error for key='{}': {}",
                key.as_str(),
                e
            ))
        })
    }

    /// Download with caching using a cache key.
    pub async fn download_cached(
        &self,
        resource: &Resource,
        key: &HlsCacheKey,
    ) -> HlsResult<CachedBytes> {
        trace!(
            "cache: request url='{}' key='{}'",
            resource.url(),
            key.as_str()
        );

        // Try to read from cache first
        if let Some(bytes) = self.read_cache(key)? {
            trace!("cache: serving from cache key='{}'", key.as_str());
            return Ok(CachedBytes {
                bytes,
                source: CacheSource::Cache,
            });
        }

        // Download from network
        trace!(
            "cache: downloading from network url='{}' key='{}'",
            resource.url(),
            key.as_str()
        );
        let bytes = self.inner.download_with_headers(resource, None).await?;

        // Write to cache
        if let Err(e) = self.write_cache(key, bytes.clone()) {
            trace!(
                "cache: failed to write to cache key='{}': {:?}",
                key.as_str(),
                e
            );
            // Don't fail the download if cache write fails
        } else {
            trace!(
                "cache: wrote to cache key='{}' ({} bytes)",
                key.as_str(),
                bytes.len()
            );
        }

        trace!(
            "cache: downloaded from network url='{}' key='{}' ({} bytes)",
            resource.url(),
            key.as_str(),
            bytes.len()
        );
        Ok(CachedBytes {
            bytes,
            source: CacheSource::Network,
        })
    }

    /// Download with caching using a cache key and custom headers.
    pub async fn download_cached_with_headers(
        &self,
        resource: &Resource,
        key: &HlsCacheKey,
        headers: Option<Headers>,
    ) -> HlsResult<CachedBytes> {
        trace!(
            "cache: request with headers url='{}' key='{}'",
            resource.url(),
            key.as_str()
        );

        // Try to read from cache first
        if let Some(bytes) = self.read_cache(key)? {
            trace!("cache: serving from cache key='{}'", key.as_str());
            return Ok(CachedBytes {
                bytes,
                source: CacheSource::Cache,
            });
        }

        // Download from network with headers
        trace!(
            "cache: downloading from network with headers url='{}' key='{}'",
            resource.url(),
            key.as_str()
        );
        let bytes = self.inner.download_with_headers(resource, headers).await?;

        // Write to cache
        if let Err(e) = self.write_cache(key, bytes.clone()) {
            trace!(
                "cache: failed to write to cache key='{}': {:?}",
                key.as_str(),
                e
            );
            // Don't fail the download if cache write fails
        } else {
            trace!(
                "cache: wrote to cache key='{}' ({} bytes)",
                key.as_str(),
                bytes.len()
            );
        }

        trace!(
            "cache: downloaded from network url='{}' key='{}' ({} bytes)",
            resource.url(),
            key.as_str(),
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
    async fn download_with_headers(
        &self,
        resource: &Resource,
        headers: Option<Headers>,
    ) -> HlsResult<Bytes> {
        // Get cache key from callback
        let key = (self.key_callback)(resource);

        if let Some(key) = key {
            // Use caching
            let cached = self
                .download_cached_with_headers(resource, &key, headers)
                .await?;
            Ok(cached.bytes)
        } else {
            // Without a cache key, we can't cache - just pass through
            self.inner.download_with_headers(resource, headers).await
        }
    }

    async fn stream(&self, resource: &Resource) -> HlsResult<ByteStream> {
        // Streaming doesn't use cache
        self.inner.stream(resource).await
    }

    async fn stream_range(
        &self,
        resource: &Resource,
        start: u64,
        end: Option<u64>,
    ) -> HlsResult<ByteStream> {
        // Streaming doesn't use cache
        self.inner.stream_range(resource, start, end).await
    }

    async fn probe_content_length(&self, resource: &Resource) -> HlsResult<Option<u64>> {
        // Probing doesn't use cache
        self.inner.probe_content_length(resource).await
    }

    fn cancel_token(&self) -> &CancellationToken {
        self.inner.cancel_token()
    }
}

impl<D: std::fmt::Debug> std::fmt::Debug for CacheDownloader<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheDownloader")
            .field("inner", &self.inner)
            .field("cache_provider", &"Arc<HlsStorageProvider>")
            .field("key_callback", &"Arc<CacheKeyCallback>")
            .finish()
    }
}

impl<D: Clone> Clone for CacheDownloader<D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cache_provider: Arc::clone(&self.cache_provider),
            key_callback: Arc::clone(&self.key_callback),
        }
    }
}
