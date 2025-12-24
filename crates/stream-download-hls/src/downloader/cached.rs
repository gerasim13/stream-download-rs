//! Cached downloader wrapper for HLS playlists and keys.
//!
//! This module provides a thin wrapper around [`crate::downloader::ResourceDownloader`] that
//! performs **read-before-fetch** caching using a [`stream_download::storage::StorageHandle`].
//!
//! Scope
//! -----
//! - Playlists (`.m3u8`) are cached as full blobs.
//! - Encryption keys are cached as full blobs.
//! - Segments are **not** cached via this layer (they are handled by segmented storage).
//!
//! Storage mapping
//! ---------------
//! The cache is addressed by [`stream_download::source::ResourceKey`]. This wrapper is intentionally
//! dumb: it treats the key as an opaque identifier and delegates lookup to `StorageHandle`.
//!
//! The HLS crate should construct keys according to its agreed tree layout, for example:
//! - playlists: `"<master_hash>/<playlist_basename>"`
//! - keys:      `"<master_hash>/<variant_id>/<key_basename>"`
//!
//! Notes
//! -----
//! - Cache behavior is "simplest possible": if present, use it; otherwise download and return.
//! - This wrapper does **not** write to cache. Persisting downloaded bytes is done by higher layers
//!   (e.g. via `StreamControl::StoreResource` handled by the storage provider).
//! - Cache read errors are treated as cache misses by default (best-effort).
//! - Cached methods return [`CachedBytes`] so callers can decide whether to persist via `StoreResource`
//!   only on network misses.
//! - This module emits `trace` logs for cache hits/misses and cache read errors.

use bytes::Bytes;
use stream_download::source::ResourceKey;
use stream_download::storage::StorageHandle;

use crate::downloader::ResourceDownloader;
use crate::error::{HlsError, HlsResult};
use tracing::trace;

/// Where the returned bytes came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheSource {
    /// The bytes were read from the storage handle (cache hit).
    Cache,
    /// The bytes were downloaded from the network (cache miss).
    Network,
}

/// Bytes returned by a cached downloader method, with metadata describing the source.
#[derive(Clone, Debug)]
pub struct CachedBytes {
    pub bytes: Bytes,
    pub source: CacheSource,
}

/// A downloader wrapper that can read playlists and keys from a `StorageHandle` before hitting the network.
#[derive(Clone, Debug)]
pub struct CachedResourceDownloader {
    inner: ResourceDownloader,
    handle: Option<StorageHandle>,
    /// If true, any cache read IO error is treated as a cache miss.
    /// If false, cache read errors are returned to the caller.
    best_effort_cache: bool,
}

impl CachedResourceDownloader {
    /// Create a new cached downloader wrapper.
    pub fn new(inner: ResourceDownloader, handle: Option<StorageHandle>) -> Self {
        Self {
            inner,
            handle,
            best_effort_cache: true,
        }
    }

    /// Convenience: create a cached downloader with cache disabled.
    pub fn new_uncached(inner: ResourceDownloader) -> Self {
        Self::new(inner, None)
    }

    /// Set whether cache read errors should be ignored (treated as miss) or surfaced.
    pub fn with_best_effort_cache(mut self, enabled: bool) -> Self {
        self.best_effort_cache = enabled;
        self
    }

    /// Access the wrapped downloader.
    pub fn inner(&self) -> &ResourceDownloader {
        &self.inner
    }

    /// Mutable access to the wrapped downloader.
    pub fn inner_mut(&mut self) -> &mut ResourceDownloader {
        &mut self.inner
    }

    /// Replace the storage handle (enable/disable caching).
    pub fn with_storage_handle(mut self, handle: Option<StorageHandle>) -> Self {
        self.handle = handle;
        self
    }

    /// Try to read from cache. Returns `Ok(Some(bytes))` for a hit, `Ok(None)` for miss.
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

    /// Download a playlist file fully into memory, using cache if available.
    ///
    /// On cache hit, returns `CacheSource::Cache` and does not hit the network.
    /// On cache miss, downloads via the inner downloader and returns `CacheSource::Network`.
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

    /// Download an encryption key fully into memory, using cache if available.
    ///
    /// On cache hit, returns `CacheSource::Cache` and does not hit the network.
    /// On cache miss, downloads via the inner downloader and returns `CacheSource::Network`.
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

    // -------------------------------------------------------------------------
    // Optional pass-through API: expose uncached behavior for existing call sites
    // -------------------------------------------------------------------------

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
