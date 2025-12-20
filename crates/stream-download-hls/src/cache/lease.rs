//! Filesystem lease guard for HLS cache streams.
//!
//! We use a simple on-disk lease file to mark that a particular HLS "stream cache root"
//! (`<cache_root>/<master_hash>/`) is currently in use by an active playback session.
//!
//! Eviction must not delete directories that are actively being written/read. The lease file
//! provides a cross-thread/process signal (best-effort) without introducing global locks.
//!
//! Layout:
//! - Master dir: `<cache_root>/<master_hash>/`
//! - Lease file: `<cache_root>/<master_hash>/.lease`
//!
//! The lease file contains a timestamp (unix seconds) and the master hash.
//!
//! Notes:
//! - Lease is best-effort. Creation failures should not break playback.
//! - Drop removes the lease file, also best-effort.
//! - To avoid "stuck leases" after crashes, we support TTL-based staleness detection.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Default TTL for a lease file.
///
/// If the process crashes and the lease file is left behind, eviction can treat it as stale
/// once it's older than this TTL.
///
/// You can override per call via [`acquire_lease_with_ttl`].
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(6 * 60 * 60); // 6 hours

/// A guard that holds a filesystem lease for a given `<cache_root>/<master_hash>` directory.
///
/// When dropped, it attempts to remove the `.lease` file.
#[derive(Debug)]
pub struct CacheLeaseGuard {
    lease_path: PathBuf,
}

impl CacheLeaseGuard {
    /// Path to the lease file (`.../.lease`).
    pub fn path(&self) -> &Path {
        &self.lease_path
    }
}

impl Drop for CacheLeaseGuard {
    fn drop(&mut self) {
        // Best-effort: never panic in Drop.
        let _ = fs::remove_file(&self.lease_path);
    }
}

/// Build the `<cache_root>/<master_hash>` directory path.
pub fn master_dir(cache_root: &Path, master_hash: &str) -> PathBuf {
    cache_root.join(master_hash)
}

/// Build the lease file path `<cache_root>/<master_hash>/.lease`.
pub fn lease_path(cache_root: &Path, master_hash: &str) -> PathBuf {
    master_dir(cache_root, master_hash).join(".lease")
}

/// Acquire a lease for `master_hash` under `cache_root` using [`DEFAULT_LEASE_TTL`].
///
/// This is best-effort. If the lease can't be created (permissions, etc.), we still return `Ok(None)`
/// so playback can proceed, but eviction won't have an active marker.
///
/// The intention is that callers keep the returned guard alive for the duration of playback.
pub fn acquire_lease(cache_root: &Path, master_hash: &str) -> io::Result<Option<CacheLeaseGuard>> {
    acquire_lease_with_ttl(cache_root, master_hash, DEFAULT_LEASE_TTL)
}

/// Acquire a lease for `master_hash` under `cache_root` with a custom TTL used for stale detection.
///
/// This function:
/// - ensures `<cache_root>/<master_hash>` exists
/// - writes/overwrites `.lease` with a fresh timestamp
///
/// It does **not** attempt to coordinate between multiple concurrent sessions; multiple writers
/// simply overwrite the lease file. This is acceptable because the semantic is "in use now".
pub fn acquire_lease_with_ttl(
    cache_root: &Path,
    master_hash: &str,
    _ttl: Duration,
) -> io::Result<Option<CacheLeaseGuard>> {
    let dir = master_dir(cache_root, master_hash);
    if let Err(_e) = fs::create_dir_all(&dir) {
        // Best-effort: don't fail playback.
        return Ok(None);
    }

    let lease = dir.join(".lease");

    // Best-effort lease content (timestamp + master hash). If this fails, just return None.
    let now = now_secs().unwrap_or(0);
    let contents = format!("ts={}\nmaster={}\n", now, master_hash);
    if fs::write(&lease, contents.as_bytes()).is_err() {
        return Ok(None);
    }

    Ok(Some(CacheLeaseGuard { lease_path: lease }))
}

/// Returns true if a lease exists and is considered "active" (not stale).
///
/// If the lease file does not exist, returns false.
/// If the lease file is unreadable/corrupt, returns true (conservative) unless it is older than TTL
/// by mtime (best-effort).
pub fn is_lease_active(cache_root: &Path, master_hash: &str, ttl: Duration) -> bool {
    let lp = lease_path(cache_root, master_hash);
    if !lp.exists() {
        return false;
    }

    // First try to read timestamp from file content.
    if let Ok(s) = fs::read_to_string(&lp) {
        if let Some(ts) = parse_ts(&s) {
            if let Ok(now) = now_secs() {
                let age = now.saturating_sub(ts);
                return age < ttl.as_secs();
            }
            // If we can't read time, be conservative.
            return true;
        }
    }

    // Fallback: use metadata modified time if available.
    match fs::metadata(&lp).and_then(|m| m.modified()) {
        Ok(modified) => match SystemTime::now().duration_since(modified) {
            Ok(age) => age < ttl,
            Err(_) => true,
        },
        Err(_) => true,
    }
}

/// Best-effort parse `ts=<unix_secs>` from lease contents.
fn parse_ts(s: &str) -> Option<u64> {
    for line in s.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("ts=") {
            if let Ok(v) = rest.trim().parse::<u64>() {
                return Some(v);
            }
        }
    }
    None
}

fn now_secs() -> io::Result<u64> {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("system time error: {e}")))?;
    Ok(dur.as_secs())
}
