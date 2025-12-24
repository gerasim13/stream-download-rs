//! Cache lease marker.
//!
//! Uses a `.lease` file under `<cache_root>/<master_hash>/`. Eviction relies on its mtime for TTL
//! freshness checks and LRU ordering. Best-effort: failures should not break playback.

use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;

use tracing::trace;

/// Default TTL for `.lease` freshness checks.
///
/// Used by the cache/eviction layer to treat old leases as inactive.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(6 * 60 * 60); // 6 hours

fn master_dir(cache_root: &Path, master_hash: &str) -> std::path::PathBuf {
    cache_root.join(master_hash)
}

fn lease_path(cache_root: &Path, master_hash: &str) -> std::path::PathBuf {
    master_dir(cache_root, master_hash).join(".lease")
}

/// Creates the `.lease` marker for `master_hash` under `cache_root` (best-effort).
///
/// Returns `Ok(true)` if the lease file exists after the call.
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

/// Touches the `.lease` file by rewriting it to bump mtime (best-effort).
///
/// Errors are returned so callers can log, but callers should not break playback.
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
