//! Cached downloader for small HLS resources.
//!
//! Provides optional read-before-fetch behavior via a `StorageHandle` and returns [`CachedBytes`]
//! tagged with [`CacheSource`].

use bytes::Bytes;
use stream_download::source::ResourceKey;
use stream_download::storage::StorageHandle;

use crate::downloader::ResourceDownloader;
use crate::error::{HlsError, HlsResult};
use tracing::trace;

/// Where returned bytes came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheSource {
    /// Read from `StorageHandle` (cache hit).
    Cache,
    /// Downloaded from the network (cache miss).
    Network,
}

/// Bytes returned by a cached method.
#[derive(Clone, Debug)]
pub struct CachedBytes {
    pub bytes: Bytes,
    pub source: CacheSource,
}

/// Cached wrapper for [`ResourceDownloader`].
///
/// If a `StorageHandle` is configured, reads from cache first; otherwise downloads from the network.
#[derive(Clone, Debug)]
pub struct CachedResourceDownloader {
    inner: ResourceDownloader,
    handle: Option<StorageHandle>,
    /// If true, cache read errors are treated as misses; otherwise they are returned.
    best_effort_cache: bool,
}

impl CachedResourceDownloader {
    /// Creates a cached downloader wrapper.
    pub fn new(inner: ResourceDownloader, handle: Option<StorageHandle>) -> Self {
        Self {
            inner,
            handle,
            best_effort_cache: true,
        }
    }

    /// Creates a wrapper with caching disabled.
    pub fn new_uncached(inner: ResourceDownloader) -> Self {
        Self::new(inner, None)
    }

    /// Controls whether cache read errors are treated as misses.
    pub fn with_best_effort_cache(mut self, enabled: bool) -> Self {
        self.best_effort_cache = enabled;
        self
    }

    /// Returns the wrapped downloader.
    pub fn inner(&self) -> &ResourceDownloader {
        &self.inner
    }

    /// Returns mutable access to the wrapped downloader.
    pub fn inner_mut(&mut self) -> &mut ResourceDownloader {
        &mut self.inner
    }

    /// Replaces the storage handle (enables/disables caching).
    pub fn with_storage_handle(mut self, handle: Option<StorageHandle>) -> Self {
        self.handle = handle;
        self
    }

    /// Reads from cache (hit => `Some`, miss => `None`).
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

    /// Downloads a playlist, using cache if available.
    pub async fn download_playlist_cached(
        &self,
        url: &str,
        key: &ResourceKey,
    ) -> HlsResult<CachedBytes> {
        trace!("playlist: request url='{}' key='{}'", url, key.0);
        if let Some(bytes) = self.read_cache(key)? {
            trace!("playlist: serving from cache key='{}'", key.0);
            return Ok(CachedBytes {
                bytes,
                source: CacheSource::Cache,
            });
        }

        trace!(
            "playlist: downloading from network url='{}' key='{}'",
            url, key.0
        );
        let bytes = self.inner.download_playlist(url).await?;
        trace!(
            "playlist: downloaded from network url='{}' key='{}' ({} bytes)",
            url,
            key.0,
            bytes.len()
        );
        Ok(CachedBytes {
            bytes,
            source: CacheSource::Network,
        })
    }

    /// Downloads an encryption key, using cache if available.
    pub async fn download_key_cached(
        &self,
        url: &str,
        key: &ResourceKey,
    ) -> HlsResult<CachedBytes> {
        trace!("key: request url='{}' key='{}'", url, key.0);
        if let Some(bytes) = self.read_cache(key)? {
            trace!("key: serving from cache key='{}'", key.0);
            return Ok(CachedBytes {
                bytes,
                source: CacheSource::Cache,
            });
        }

        trace!(
            "key: downloading from network url='{}' key='{}'",
            url, key.0
        );
        let bytes = self.inner.download_key(url).await?;
        trace!(
            "key: downloaded from network url='{}' key='{}' ({} bytes)",
            url,
            key.0,
            bytes.len()
        );
        Ok(CachedBytes {
            bytes,
            source: CacheSource::Network,
        })
    }

    // Optional pass-through API for uncached behavior.

    /// Download bytes from a URL fully into memory (no cache).
    pub async fn download_bytes(&self, url: &str) -> HlsResult<Bytes> {
        self.inner.download_bytes(url).await
    }

    /// Download a playlist file fully into memory (no cache).
    pub async fn download_playlist(&self, url: &str) -> HlsResult<Bytes> {
        self.inner.download_playlist(url).await
    }

    /// Download an encryption key fully into memory (no cache).
    pub async fn download_key(&self, url: &str) -> HlsResult<Bytes> {
        self.inner.download_key(url).await
    }
}
