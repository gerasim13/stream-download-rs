//! Tree-layout storage handle for persisted HLS resources.
//!
//! This implements a `stream_download::storage::StorageResourceReader` that maps `ResourceKey`
//! directly to a file path under a configured `storage_root`.
//!
//! Mapping rules
//! ------------
//! - `ResourceKey` is treated as a relative path (slash-separated components).
//! - Each component is sanitized to prevent path traversal and weird filesystem edge cases.
//! - The final path is `storage_root / sanitized_components...`.
//!
//! This is intentionally simple and index-free: existence of the file on disk is the cache.
//!
//! Intended usage
//! --------------
//! - HLS manager/downloader cache layer calls `handle.read(&key)` before going to the network.
//! - On cache hit, it can use the returned bytes (e.g. for playlists/keys).
//!
//! Notes
//! -----
//! - This is designed for small resources (playlists/keys). It reads the entire file into memory.
//! - For segments (large), the segmented storage reader path is preferred (streamed `Read+Seek`).
//! - This is best-effort: missing/empty files are treated as cache miss (`Ok(None)`).

use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use stream_download::source::ResourceKey;
use stream_download::storage::{StorageHandle, StorageResourceReader};

/// A tree-layout resource reader rooted at `storage_root`.
#[derive(Clone, Debug)]
pub struct TreeStorageResourceReader {
    storage_root: PathBuf,
}

impl TreeStorageResourceReader {
    /// Create a new tree-layout reader rooted at `storage_root`.
    pub fn new(storage_root: impl Into<PathBuf>) -> Self {
        Self {
            storage_root: storage_root.into(),
        }
    }

    /// Root directory where resources are stored.
    pub fn storage_root(&self) -> &Path {
        &self.storage_root
    }

    /// Create a `StorageHandle` for this reader.
    pub fn into_handle(self) -> StorageHandle {
        StorageHandle::new(Arc::new(self))
    }

    /// Convert a `ResourceKey` into an on-disk path under `storage_root`.
    ///
    /// This function:
    /// - splits key by `/`
    /// - sanitizes each component
    /// - joins them under `storage_root`
    pub fn path_for_key(&self, key: &ResourceKey) -> PathBuf {
        let mut out = self.storage_root.clone();

        // Treat ResourceKey as a relative path.
        // Split by '/' to avoid platform-specific path parsing differences.
        for raw in key.0.split('/') {
            let clean = sanitize_component(raw);
            if clean.is_empty() {
                continue;
            }
            out.push(clean);
        }

        out
    }
}

impl StorageResourceReader for TreeStorageResourceReader {
    fn read(&self, key: &ResourceKey) -> io::Result<Option<Bytes>> {
        let path = self.path_for_key(key);

        // If it doesn't exist, it's a cache miss.
        if !path.exists() {
            return Ok(None);
        }

        // If it's a directory, treat as miss.
        if path.is_dir() {
            return Ok(None);
        }

        let data = std::fs::read(&path)?;
        if data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Bytes::from(data)))
        }
    }

    fn exists(&self, key: &ResourceKey) -> io::Result<bool> {
        let path = self.path_for_key(key);

        // Treat missing paths as miss.
        if !path.exists() {
            return Ok(false);
        }

        // Treat directories as miss.
        if path.is_dir() {
            return Ok(false);
        }

        Ok(true)
    }

    fn len(&self, key: &ResourceKey) -> io::Result<Option<u64>> {
        let path = self.path_for_key(key);

        // Treat missing paths as miss.
        if !path.exists() {
            return Ok(None);
        }

        // Treat directories as miss.
        if path.is_dir() {
            return Ok(None);
        }

        let meta = std::fs::metadata(&path)?;
        Ok(Some(meta.len()))
    }
}

/// Sanitize a single path component so it is safe to use under `storage_root`.
///
/// Goals:
/// - prevent path traversal (`..`)
/// - disallow absolute paths
/// - keep filenames stable-ish
///
/// Strategy:
/// - allow ASCII alnum plus `._-`
/// - replace everything else with `_`
/// - special-case `.` and `..` as empty
fn sanitize_component(s: &str) -> String {
    // Fast reject: `.` and `..` should never be used as a component.
    if s == "." || s == ".." {
        return String::new();
    }

    // Also reject components that would be interpreted as absolute paths on some platforms.
    // We do this by using Path parsing and rejecting any prefix/root/parent-dir components.
    let p = Path::new(s);
    for c in p.components() {
        match c {
            Component::Normal(_) => {}
            // Prefix (Windows), RootDir, CurDir, ParentDir are not allowed.
            _ => return String::new(),
        }
    }

    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}
