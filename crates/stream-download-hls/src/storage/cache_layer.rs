//! Cache coordination layer for HLS persistent storage.
//!
//! This module provides a wrapper around an inner [`SegmentStorageFactory`] that adds:
//! - A filesystem "lease" marker to prevent eviction of currently active streams.
//! - LRU-style eviction by `<storage_root>/<master_hash>` directory count.
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
//! Marker:
//! - lease: `<storage_root>/<master_hash>/.lease`
//!
//! The lease file serves two roles:
//! - **Active-use protection**: eviction must not delete dirs with a fresh lease (mtime < TTL).
//! - **LRU ordering**: eviction sorts master dirs by lease mtime (oldest evicted first).
//!
//! There is intentionally no separate ".access" marker and no in-memory lease guard.
//! The lease file is not removed on shutdown; TTL-based eviction handles cleanup.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use stream_download::source::{ChunkKind, ResourceKey};
use stream_download::storage::{ProvidesStorageHandle, StorageHandle};

use crate::cache::lease::{DEFAULT_LEASE_TTL, ensure_lease_marker, touch_lease_best_effort};
use crate::storage::tree_handle::TreeStorageResourceReader;

use super::SegmentStorageFactory;

use tracing::trace;

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
    // Master hashes we've already initialized in this process.
    seen_masters: HashSet<String>,
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

    /// Best-effort read of lease mtime used for LRU ordering.
    ///
    /// Returns `0` if there is no lease or metadata cannot be read.
    fn lease_mtime_ts(&self, master_dir: &Path) -> u64 {
        let lease = master_dir.join(".lease");
        let Ok(meta) = fs::metadata(&lease) else {
            return 0;
        };
        let Ok(modified) = meta.modified() else {
            return 0;
        };
        match modified.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(d) => d.as_secs(),
            Err(_) => 0,
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
                trace!("cache: lease create start master_hash='{}'", master_hash);

                // First time we see this master_hash in this process: ensure the lease marker exists.
                // The lease file is *not* removed on shutdown; eviction uses mtime + TTL.
                match ensure_lease_marker(&self.storage_root, master_hash) {
                    Ok(true) => trace!("cache: lease create OK master_hash='{}'", master_hash),
                    Ok(false) => trace!(
                        "cache: lease create SKIP/FAIL master_hash='{}' (best-effort)",
                        master_hash
                    ),
                    Err(e) => trace!(
                        "cache: lease create ERROR master_hash='{}' err='{}' (best-effort)",
                        master_hash, e
                    ),
                }

                // Only consider eviction once per newly observed master.
                run_eviction = true;
            }
        }

        // Touch lease every time (LRU + keep fresh relative to TTL).
        match touch_lease_best_effort(&self.storage_root, master_hash) {
            Ok(()) => trace!("cache: lease touch OK master_hash='{}'", master_hash),
            Err(e) => trace!(
                "cache: lease touch ERROR master_hash='{}' err='{}' (best-effort)",
                master_hash, e
            ),
        }

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
                    trace!("cache: evict skip protected dir='{}'", path.display());
                    continue;
                }
            }

            // Do not evict active leases (unless stale by TTL).
            if self.is_lease_active_path(&path) {
                trace!("cache: evict skip active-lease dir='{}'", path.display());
                continue;
            }

            let ts = self.lease_mtime_ts(&path);
            trace!(
                "cache: evict candidate dir='{}' lease_mtime_ts={}",
                path.display(),
                ts
            );
            masters.push((ts, path));
        }

        let max = max.get();
        if masters.len() <= max {
            trace!(
                "cache: evict not-needed masters={} max={}",
                masters.len(),
                max
            );
            return Ok(());
        }

        // Oldest first.
        masters.sort_by_key(|(ts, _)| *ts);

        let to_remove = masters.len().saturating_sub(max);
        trace!(
            "cache: evict start masters={} max={} to_remove={}",
            masters.len(),
            max,
            to_remove
        );

        for (_, dir) in masters.into_iter().take(to_remove) {
            trace!("cache: evict removing dir='{}'", dir.display());
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
