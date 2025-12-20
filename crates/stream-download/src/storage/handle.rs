//! Core storage handle APIs for keyed resource read caching.
//!
//! Motivation
//! ----------
//! Some protocols (e.g. HLS/DASH) deal with *many* small auxiliary resources in addition to the
//! primary byte stream (playlists, init segments, encryption keys, etc.).
//!
//! While the primary stream is handled via `StorageProvider` (reader+writer over a single backing
//! resource), auxiliary resources benefit from a *keyed*, read-only interface that higher layers
//! can query *before* going to the network.
//!
//! This module defines a minimal, lock-free contract:
//! - `StorageResourceReader`: "given a `ResourceKey`, return the full bytes if available"
//! - `StorageHandle`: a cheap cloneable wrapper around an implementation
//! - `ProvidesStorageHandle`: optional trait for providers to vend such handles

use std::fmt;
use std::io;
use std::sync::Arc;

use bytes::Bytes;

use crate::source::ResourceKey;

/// Read-only interface for retrieving whole resources addressed by [`ResourceKey`].
///
/// Notes:
/// - The semantics are "read the full resource blob" (small objects like playlists / keys).
/// - Returning `Ok(None)` indicates cache miss / absent resource.
/// - Implementations should be fast and should avoid global locks; callers may invoke this
///   opportunistically before network requests.
pub trait StorageResourceReader: Send + Sync + 'static {
    /// Read a full resource blob by key.
    ///
    /// Returns:
    /// - `Ok(Some(bytes))` if the resource exists in storage.
    /// - `Ok(None)` if the resource is not present (cache miss).
    /// - `Err(e)` on IO/read errors.
    fn read(&self, key: &ResourceKey) -> io::Result<Option<Bytes>>;
}

/// A cheap, cloneable handle for keyed resource reads.
///
/// This is typically created by a storage provider (or a higher-level cache layer) and passed to
/// protocol-specific downloaders/managers so they can do read-before-fetch caching.
#[derive(Clone)]
pub struct StorageHandle {
    inner: Arc<dyn StorageResourceReader>,
}

impl fmt::Debug for StorageHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageHandle").finish_non_exhaustive()
    }
}

impl StorageHandle {
    /// Create a new storage handle from a resource reader implementation.
    pub fn new(inner: Arc<dyn StorageResourceReader>) -> Self {
        Self { inner }
    }

    /// Read a resource by key, returning its full bytes if present.
    #[inline]
    pub fn read(&self, key: &ResourceKey) -> io::Result<Option<Bytes>> {
        self.inner.read(key)
    }
}

/// Optional trait for storage providers (or wrappers) that can vend a [`StorageHandle`].
///
/// This is intentionally separate from `StorageProvider` so existing providers are not forced to
/// implement it. Protocol crates can require it when they need read-before-fetch caching.
pub trait ProvidesStorageHandle {
    /// Return a read-only [`StorageHandle`] for keyed resource reads, if supported.
    ///
    /// Providers that do not support keyed reads should return `None`.
    fn storage_handle(&self) -> Option<StorageHandle>;
}
