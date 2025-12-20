//! Filesystem lease marker for HLS cache streams.
//!
//! We use a simple on-disk lease file to mark activity for a particular HLS "stream cache root":
//! `<cache_root>/<master_hash>/`.
//!
//! In this design, the lease file serves two roles:
//! 1) **Active-use protection**: eviction must not delete dirs with a fresh lease (mtime < TTL).
//! 2) **LRU ordering**: eviction sorts master dirs by the lease file's **mtime**
//!    (oldest evicted first).
//!
//! There is intentionally no separate ".access" marker and no in-memory lease guard.
//!
//! Layout:
//! - Master dir: `<cache_root>/<master_hash>/`
//! - Lease file: `<cache_root>/<master_hash>/.lease`
//!
//! Notes:
//! - Best-effort: failures must not break playback.
//! - The lease file is NOT removed on shutdown. TTL-based eviction handles cleanup and crash cases.

use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;

use tracing::trace;

/// Default TTL for a lease file.
///
/// If the process crashes and the lease file is left behind, eviction can treat it as stale
/// once it's older than this TTL.
///
/// This value is used by the cache/eviction layer when interpreting `.lease` freshness.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(6 * 60 * 60); // 6 hours

fn master_dir(cache_root: &Path, master_hash: &str) -> std::path::PathBuf {
    cache_root.join(master_hash)
}

fn lease_path(cache_root: &Path, master_hash: &str) -> std::path::PathBuf {
    master_dir(cache_root, master_hash).join(".lease")
}

/// Create the lease marker file for `master_hash` under `cache_root`.
///
/// This is best-effort:
/// - returns `Ok(false)` if the lease couldn't be created (permissions, etc.)
/// - returns `Ok(true)` if the lease file exists after the call
///
/// The content of the file is irrelevant; eviction uses the file's **mtime**.
/// The file is not removed automatically.
pub fn ensure_lease_marker(cache_root: &Path, master_hash: &str) -> io::Result<bool> {
    let dir = master_dir(cache_root, master_hash);
    if fs::create_dir_all(&dir).is_err() {
        trace!(
            "cache: lease mkdir FAIL cache_root='{}' master_hash='{}' (best-effort)",
            cache_root.display(),
            master_hash
        );
        return Ok(false);
    }

    let lease = lease_path(cache_root, master_hash);

    // Best-effort marker creation; mtime is what matters.
    if fs::write(&lease, b"lease=1\n").is_err() {
        trace!(
            "cache: lease create FAIL path='{}' master_hash='{}' (best-effort)",
            lease.display(),
            master_hash
        );
        return Ok(false);
    }

    trace!(
        "cache: lease create OK path='{}' master_hash='{}'",
        lease.display(),
        master_hash
    );
    Ok(true)
}

/// Best-effort "touch" of the lease file for LRU ordering.
///
/// This updates the lease file mtime by rewriting the file, ensuring:
/// - active streams stay "fresh" relative to TTL-based staleness checks,
/// - LRU eviction can sort by lease mtime without a separate access marker.
///
/// Failures are returned as errors so the caller can log, but callers should treat them as
/// best-effort and not break playback.
pub fn touch_lease_best_effort(cache_root: &Path, master_hash: &str) -> io::Result<()> {
    let lease = lease_path(cache_root, master_hash);

    // Ensure parent exists (best-effort).
    if let Some(parent) = lease.parent() {
        let _ = fs::create_dir_all(parent);
    }

    trace!(
        "cache: lease touch path='{}' master_hash='{}'",
        lease.display(),
        master_hash
    );

    // Rewrite the file to bump mtime. Content is irrelevant.
    fs::write(&lease, b"lease=1\n")?;
    Ok(())
}
