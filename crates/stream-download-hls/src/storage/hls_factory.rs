//! Built-in HLS storage layout helpers.
//!
//! This module provides a production-oriented segment storage factory that implements a
//! per-segment **file tree layout** rooted at a single `storage_root` directory.
//!
//! Target layout (segments):
//! `<storage_root>/<master_hash>/<variant_id>/<segment_basename>`
//!
//! Important note about `stream_key`:
//! - The segmented orchestrator only knows about `stream_key` and `chunk_id`.
//! - “Master playlist hash” and variant id are HLS-level concepts.
//! - To keep the storage orchestrator protocol-agnostic, this factory expects that the HLS layer
//!   encodes `stream_key` as `"<master_hash>/<variant_id>"`.
//!
//! Important note about filenames/extensions:
//! - Segment filenames (and their extensions) must come from the playlist URIs.
//! - Do NOT synthesize extensions based on `ChunkKind`.
//! - If the playlist URI has no extension, the cached file should also have no extension.
//!
//! This factory supports an optional `filename_hint` (playlist-derived basename, query ignored).
//! If present, it is used "as is" as the segment filename (no extension inference).
//! If absent (or empty), we return an error to surface the bug immediately.
//!
//! NOTE:
//! - Eviction / leases / LRU policy are implemented in the cache layer wrapper (`HlsCacheLayer`).
//!   This file-tree factory is intentionally "dumb": it only maps a segment to a deterministic path.

use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use stream_download::source::{ChunkKind, ResourceKey};
use stream_download::storage::adaptive::AdaptiveStorageProvider;
use stream_download::storage::file::FileStorageProvider;

use crate::storage::SegmentStorageFactory;

/// Built-in HLS segment storage factory that writes each segment to its own file.
///
/// Expected `stream_key` convention:
/// - `"<master_hash>/<variant_id>"`
///
/// Where `master_hash` is a stable identifier derived from the *master playlist URL*
/// (not from each individual segment URL).
#[derive(Clone, Debug)]
pub struct HlsFileTreeSegmentFactory {
    storage_root: PathBuf,
    /// Base prefetch buffer size used by the adaptive storage wrapper.
    ///
    /// The factory will multiply it by 2 (consistent with previous examples).
    prefetch_bytes: NonZeroUsize,
}

impl HlsFileTreeSegmentFactory {
    /// Create a new factory rooted at `storage_root`.
    pub fn new(storage_root: impl Into<PathBuf>, prefetch_bytes: NonZeroUsize) -> Self {
        Self {
            storage_root: storage_root.into(),
            prefetch_bytes,
        }
    }

    /// Root directory where all persistent segment files will live.
    pub fn storage_root(&self) -> &Path {
        &self.storage_root
    }

    /// Parse `stream_key` as `"<master_hash>/<variant_id>"`.
    fn parse_stream_key<'a>(&self, stream_key: &'a ResourceKey) -> io::Result<(&'a str, &'a str)> {
        let s: &str = &stream_key.0;

        let (master_hash, variant_id) = s.split_once('/').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream_key must be in format '<master_hash>/<variant_id>'",
            )
        })?;

        if master_hash.is_empty() || variant_id.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream_key must be in format '<master_hash>/<variant_id>' (non-empty parts)",
            ));
        }

        Ok((master_hash, variant_id))
    }

    fn segment_path(
        &self,
        stream_key: &ResourceKey,
        chunk_id: u64,
        _kind: Option<ChunkKind>,
        filename_hint: Option<&str>,
    ) -> io::Result<PathBuf> {
        let (master_hash, variant_id) = self.parse_stream_key(stream_key)?;

        // Use the playlist-derived basename.
        // We intentionally do not try to "fix" extensions here: the caller must pass the exact
        // basename they want on disk (query ignored upstream).
        let filename = match filename_hint {
            Some(hint) if !hint.is_empty() => hint.to_string(),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "missing/empty filename_hint for HLS segment: stream_key='{}' chunk_id={}",
                        stream_key.0, chunk_id
                    ),
                ));
            }
        };

        Ok(self
            .storage_root
            .join(master_hash)
            .join(variant_id)
            .join(filename))
    }

    /// Create a per-segment storage provider, writing bytes to:
    /// `<storage_root>/<master_hash>/<variant_id>/<segment_basename>`.
    pub fn provider_for_segment(
        &self,
        stream_key: &ResourceKey,
        chunk_id: u64,
        kind: Option<ChunkKind>,
        filename_hint: Option<&str>,
    ) -> io::Result<AdaptiveStorageProvider<FileStorageProvider, FileStorageProvider>> {
        let path = self.segment_path(stream_key, chunk_id, kind, filename_hint)?;

        // `FileStorageProvider::open` creates the parent directories, but we keep this here to
        // provide a better error context at the factory level.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Use adaptive storage on top of a per-segment file. This keeps behavior consistent with
        // existing buffering logic while ensuring each segment is a separate file.
        let buffer = NonZeroUsize::new(self.prefetch_bytes.get().saturating_mul(2))
            .unwrap_or(self.prefetch_bytes);

        Ok(AdaptiveStorageProvider::new(
            FileStorageProvider::open(&path)?,
            buffer,
        ))
    }
}

impl SegmentStorageFactory for HlsFileTreeSegmentFactory {
    type Provider = AdaptiveStorageProvider<FileStorageProvider, FileStorageProvider>;

    fn provider_for_segment(
        &self,
        stream_key: &ResourceKey,
        chunk_id: u64,
        kind: ChunkKind,
        filename_hint: Option<&str>,
    ) -> io::Result<Self::Provider> {
        self.provider_for_segment(stream_key, chunk_id, Some(kind), filename_hint)
    }
}
