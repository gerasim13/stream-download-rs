//! HLS file-tree segment storage factory.
//!
//! Maps each segment to a deterministic path under `storage_root` and requires `stream_key` to be
//! `"<master_hash>/<variant_id>"`.

use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use stream_download::source::{ChunkKind, ResourceKey};
use stream_download::storage::adaptive::AdaptiveStorageProvider;
use stream_download::storage::file::FileStorageProvider;

use crate::storage::SegmentStorageFactory;

/// Writes each segment to its own file under `storage_root`.
///
/// Requires `stream_key` to be `"<master_hash>/<variant_id>"`.
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
