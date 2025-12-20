//! Cache coordination layer for HLS persistent storage.
//!
//! This module provides a wrapper around an inner [`SegmentStorageFactory`] that adds:
//! - A filesystem "lease" marker to prevent eviction of currently active streams.
//! - LRU-style eviction by `<storage_root>/<master_hash>` directory count.
//! - `.access` marker updates for LRU ordering.
//!
//! Additionally, it can vend a read-only [`StorageHandle`] for persisted small resources
//! (e.g. playlists/keys) stored under `storage_root` by treating `ResourceKey` as a relative path.
//!
//! Design goals:
//! - Keep all caching policy out of `HlsStream` / playback logic.
//! - Avoid global locks and avoid hot-path overhead:
//!   eviction is triggered only when a *new* `<master_hash>` is first encountered.
//! - Best-effort: cache policy must not break playback.
//!
//! Storage layout assumptions (aligned with `HlsFileTreeSegmentFactory`):
//! - `stream_key` is `"<master_hash>/<variant_id>"`
//! - segment path: `<storage_root>/<master_hash>/<variant_id>/<segment_basename>`
//!
//! Markers:
//! - lease:  `<storage_root>/<master_hash>/.lease`
//! - access: `<storage_root>/<master_hash>/.access`

use std::collections::HashSet;
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use stream_download::source::{ChunkKind, ResourceKey};
use stream_download::storage::{ProvidesStorageHandle, StorageHandle};

use crate::cache::lease::{CacheLeaseGuard, DEFAULT_LEASE_TTL};
use crate::storage::tree_handle::TreeStorageResourceReader;

use super::SegmentStorageFactory;

/// A cache-policy wrapper for an HLS segment storage factory.
///
/// This wrapper:
/// - Parses `master_hash` out of the HLS `stream_key` (`"<master_hash>/<variant_id>"`).
/// - Acquires a lease for each encountered `master_hash` to prevent eviction while active.
/// - Updates an access marker file for LRU ordering.
/// - Evicts older stream directories when the count exceeds `max_cached_streams`.
#[derive(Clone)]
pub struct HlsCacheLayer<F>
where
    F: SegmentStorageFactory,
{
    inner: F,
    storage_root: PathBuf,

    max_cached_streams: Option<NonZeroUsize>,
    lease_ttl: Duration,

    // State is shared across clones (factory is cloned into multiple places).
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    // Master hashes we've already initialized in this process (lease acquired and LRU touched).
    seen_masters: HashSet<String>,

    // Keep lease guards alive for the lifetime of this cache layer.
    // Note: multiple concurrent playbacks may coexist; we keep one guard per master_hash.
    leases: Vec<CacheLeaseGuard>,
}

impl<F> HlsCacheLayer<F>
where
    F: SegmentStorageFactory,
{
    /// Create a new cache layer around `inner`.
    ///
    /// `storage_root` must match the root used by the inner factory layout, e.g.
    /// `<storage_root>/<master_hash>/<variant_id>/<segment_basename>`.
    pub fn new(inner: F, storage_root: impl Into<PathBuf>) -> Self {
        Self {
            inner,
            storage_root: storage_root.into(),
            max_cached_streams: None,
            lease_ttl: DEFAULT_LEASE_TTL,
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// Return the root directory where stream caches are stored.
    pub fn storage_root(&self) -> &Path {
        &self.storage_root
    }

    fn make_storage_handle(&self) -> StorageHandle {
        TreeStorageResourceReader::new(self.storage_root.clone()).into_handle()
    }

    /// Enable LRU eviction by master playlist (by `<storage_root>/<master_hash>` directory count).
    pub fn with_max_cached_streams(mut self, max: NonZeroUsize) -> Self {
        self.max_cached_streams = Some(max);
        self
    }

    /// Override the lease TTL used to treat stale `.lease` files as inactive.
    pub fn with_lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }

    fn parse_master_hash<'a>(&self, stream_key: &'a ResourceKey) -> io::Result<&'a str> {
        let s: &str = &stream_key.0;
        let (master_hash, _variant_id) = s.split_once('/').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream_key must be in format '<master_hash>/<variant_id>'",
            )
        })?;

        if master_hash.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream_key must include a non-empty <master_hash>",
            ));
        }

        Ok(master_hash)
    }

    fn master_dir(&self, master_hash: &str) -> PathBuf {
        self.storage_root.join(master_hash)
    }

    fn access_marker_path(master_dir: &Path) -> PathBuf {
        master_dir.join(".access")
    }

    fn now_secs() -> io::Result<u64> {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("system time error: {e}")))?;
        Ok(dur.as_secs())
    }

    fn touch_access_marker(&self, master_hash: &str) -> io::Result<()> {
        let dir = self.master_dir(master_hash);
        fs::create_dir_all(&dir)?;
        let ts = Self::now_secs()?;
        fs::write(Self::access_marker_path(&dir), ts.to_string().as_bytes())?;
        Ok(())
    }

    fn read_access_ts(&self, master_dir: &Path) -> u64 {
        let marker = Self::access_marker_path(master_dir);
        let Ok(data) = fs::read_to_string(marker) else {
            return 0;
        };
        data.trim().parse::<u64>().unwrap_or(0)
    }

    fn is_lease_active_path(&self, master_dir: &Path) -> bool {
        let lease = master_dir.join(".lease");
        if !lease.exists() {
            return false;
        }

        // Prefer mtime-based TTL (cheap).
        let Ok(meta) = fs::metadata(&lease) else {
            return true;
        };
        let Ok(modified) = meta.modified() else {
            return true;
        };
        match SystemTime::now().duration_since(modified) {
            Ok(age) => age < self.lease_ttl,
            Err(_) => true,
        }
    }

    /// Attempt to ensure cache state for `master_hash`:
    /// - acquire lease (keep guard alive)
    /// - touch LRU access marker
    /// - possibly evict older masters (only when first seeing this master)
    ///
    /// Best-effort: failures are ignored to avoid breaking playback.
    fn ensure_master_initialized_best_effort(&self, master_hash: &str) {
        let mut run_eviction = false;

        {
            let mut st = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());

            if st.seen_masters.insert(master_hash.to_string()) {
                // First time we see this master_hash in this process: acquire lease.
                if let Ok(Some(guard)) =
                    crate::cache::lease::acquire_lease(&self.storage_root, master_hash)
                {
                    st.leases.push(guard);
                }

                // Only consider eviction once per newly observed master.
                run_eviction = true;
            }
        }

        // Touch access marker every time (LRU).
        let _ = self.touch_access_marker(master_hash);

        if run_eviction {
            let _ = self.evict_if_needed_best_effort(Some(master_hash));
        }
    }

    /// Best-effort eviction implementation.
    ///
    /// `protect_master_hash` is never removed (even if not leased), to avoid evicting the stream
    /// we're currently initializing.
    fn evict_if_needed_best_effort(&self, protect_master_hash: Option<&str>) -> io::Result<()> {
        let Some(max) = self.max_cached_streams else {
            return Ok(());
        };

        let entries = match fs::read_dir(&self.storage_root) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Ok(()), // best-effort
        };

        let mut masters: Vec<(u64, PathBuf)> = Vec::new();

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };

            let path = entry.path();

            let ft = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };
            if !ft.is_dir() {
                continue;
            }

            // Never delete the protected master dir.
            if let Some(protect) = protect_master_hash {
                if path.file_name().and_then(|s| s.to_str()) == Some(protect) {
                    continue;
                }
            }

            // Do not evict active leases (unless stale by TTL).
            if self.is_lease_active_path(&path) {
                continue;
            }

            let ts = self.read_access_ts(&path);
            masters.push((ts, path));
        }

        let max = max.get();
        if masters.len() <= max {
            return Ok(());
        }

        // Oldest first.
        masters.sort_by_key(|(ts, _)| *ts);

        let to_remove = masters.len().saturating_sub(max);
        for (_, dir) in masters.into_iter().take(to_remove) {
            // Best-effort remove.
            let _ = fs::remove_dir_all(dir);
        }

        Ok(())
    }
}

impl<F> SegmentStorageFactory for HlsCacheLayer<F>
where
    F: SegmentStorageFactory,
{
    type Provider = F::Provider;

    fn provider_for_segment(
        &self,
        stream_key: &ResourceKey,
        chunk_id: u64,
        kind: ChunkKind,
        filename_hint: Option<&str>,
    ) -> io::Result<Self::Provider> {
        // Initialize cache policy for the master hash (best-effort).
        if let Ok(master_hash) = self.parse_master_hash(stream_key) {
            // If desired, we could restrict this to Init chunks only. But we keep it simple and safe:
            // the `seen_masters` set ensures all heavy work happens only once per master.
            let _ = (chunk_id, kind); // chunk_id/kind are not needed for policy right now.
            self.ensure_master_initialized_best_effort(master_hash);
        }

        // Delegate actual storage creation to the inner factory.
        self.inner
            .provider_for_segment(stream_key, chunk_id, kind, filename_hint)
    }
}

impl<F> ProvidesStorageHandle for HlsCacheLayer<F>
where
    F: SegmentStorageFactory,
{
    fn storage_handle(&self) -> Option<StorageHandle> {
        Some(self.make_storage_handle())
    }
}
