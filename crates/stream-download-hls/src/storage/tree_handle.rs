//! Tree-layout reader for small persisted resources.
//!
//! Maps a [`ResourceKey`] to a sanitized relative path under `storage_root`.
//! Intended for small blobs (e.g. playlists/keys): reads the whole file into memory.
//! Missing/empty files are treated as cache misses (`Ok(None)`).

use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use stream_download::source::ResourceKey;
use stream_download::storage::{StorageHandle, StorageResourceReader};

/// Tree-layout resource reader rooted at `storage_root`.
#[derive(Clone, Debug)]
pub struct TreeStorageResourceReader {
    storage_root: PathBuf,
}

impl TreeStorageResourceReader {
    /// Creates a new tree-layout reader rooted at `storage_root`.
    pub fn new(storage_root: impl Into<PathBuf>) -> Self {
        Self {
            storage_root: storage_root.into(),
        }
    }

    /// Creates a `StorageHandle` for this reader.
    pub fn into_handle(self) -> StorageHandle {
        StorageHandle::new(Arc::new(self))
    }

    /// Converts a [`ResourceKey`] into an on-disk path under `storage_root`.
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
