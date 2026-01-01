//! New segmented storage provider with cache support.
//!
//! This module provides a modern segmented storage implementation that uses
//! LeaseAwareCacheTree for caching HLS resources while providing StorageProvider
//! interface for reading/writing current segments.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use stream_download::storage::StorageProvider;

use crate::cache::keys::HlsCacheKey;
use crate::storage_new::HlsStorageProvider;

/// State for a single stream within the segmented storage.
pub(crate) struct StreamState {
    /// Current segment index being read/written
    current_segment: u64,
    /// Map of segment index to file path
    segments: HashMap<u64, PathBuf>,
    /// Current position within the current segment
    position: u64,
}

impl Default for StreamState {
    fn default() -> Self {
        Self {
            current_segment: 0,
            segments: HashMap::new(),
            position: 0,
        }
    }
}

/// Reader for segmented storage.
pub struct SegmentedReader {
    /// Shared state with the writer
    state: Arc<Mutex<StreamState>>,
    /// Cache for reading segments
    cache: Arc<HlsStorageProvider>,
}

impl SegmentedReader {
    /// Create a new segmented reader.
    pub fn new(state: Arc<Mutex<StreamState>>, cache: Arc<HlsStorageProvider>) -> Self {
        Self { state, cache }
    }
}

impl io::Read for SegmentedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut state = self.state.lock().unwrap();

        // Get current segment path
        let segment_path = state
            .segments
            .get(&state.current_segment)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Segment not found"))?;

        // Create cache key for this segment
        let cache_key = HlsCacheKey::new(segment_path.to_string_lossy().into_owned());

        // Try to read from cache
        if let Some(cached_data) = self.cache.get(&cache_key)? {
            let start_pos = state.position as usize;
            let end_pos = (state.position as usize + buf.len()).min(cached_data.len());

            if start_pos >= cached_data.len() {
                return Ok(0); // EOF
            }

            let bytes_to_copy = end_pos - start_pos;
            buf[..bytes_to_copy].copy_from_slice(&cached_data[start_pos..end_pos]);
            state.position += bytes_to_copy as u64;

            Ok(bytes_to_copy)
        } else {
            // Segment not in cache
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Segment not cached",
            ))
        }
    }
}

impl io::Seek for SegmentedReader {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let mut state = self.state.lock().unwrap();

        match pos {
            io::SeekFrom::Start(offset) => {
                state.position = offset;
            }
            io::SeekFrom::End(_offset) => {
                // We don't know total length, so can't seek from end
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "Seek from end not supported for segmented storage",
                ));
            }
            io::SeekFrom::Current(offset) => {
                if offset >= 0 {
                    state.position = state.position.saturating_add(offset as u64);
                } else {
                    state.position = state.position.saturating_sub((-offset) as u64);
                }
            }
        }

        Ok(state.position)
    }
}

// StorageReader is automatically implemented for types that implement Read + Seek + Send
// No need for explicit implementation

/// Writer for segmented storage.
pub struct SegmentedWriter {
    /// Shared state with the reader
    state: Arc<Mutex<StreamState>>,
    /// Cache for storing segments
    cache: Arc<HlsStorageProvider>,
    /// Current segment being written
    current_segment: u64,
    /// Buffer for current segment data
    buffer: Vec<u8>,
}

impl SegmentedWriter {
    /// Create a new segmented writer.
    pub fn new(state: Arc<Mutex<StreamState>>, cache: Arc<HlsStorageProvider>) -> Self {
        Self {
            state,
            cache,
            current_segment: 0,
            buffer: Vec::new(),
        }
    }

    /// Finalize current segment and move to next one.
    pub fn finalize_current_segment(&mut self, segment_key: &HlsCacheKey) -> io::Result<()> {
        if !self.buffer.is_empty() {
            // Store current segment in cache
            self.cache
                .put(segment_key, Bytes::from(self.buffer.clone()))?;

            // Update state
            let mut state = self.state.lock().unwrap();
            state
                .segments
                .insert(self.current_segment, PathBuf::from(segment_key.as_str()));

            // Clear buffer and move to next segment
            self.buffer.clear();
            self.current_segment += 1;
            state.current_segment = self.current_segment;
        }

        Ok(())
    }
}

impl io::Write for SegmentedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // Nothing to flush - data is buffered until segment is finalized
        Ok(())
    }
}

impl io::Seek for SegmentedWriter {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        // Writers typically don't seek within segments
        // They write sequentially and finalize segments
        match pos {
            io::SeekFrom::Start(0) => {
                // Reset to start of current segment
                self.buffer.clear();
                Ok(0)
            }
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Seek not supported for segmented writer",
            )),
        }
    }
}

// StorageWriter is automatically implemented for types that implement Write + Seek + Send + 'static
// No need for explicit implementation

/// Segmented storage provider with cache support.
pub struct SegmentedStorageProvider {
    /// Cache for HLS resources
    cache: Arc<HlsStorageProvider>,
    /// Shared state between reader and writer
    state: Arc<Mutex<StreamState>>,
}

impl SegmentedStorageProvider {
    /// Create a new segmented storage provider.
    pub fn new(cache: HlsStorageProvider) -> Self {
        Self {
            cache: Arc::new(cache),
            state: Arc::new(Mutex::new(StreamState::default())),
        }
    }

    /// Get a reference to the cache.
    pub fn cache(&self) -> &Arc<HlsStorageProvider> {
        &self.cache
    }

    /// Create a cache key for a segment.
    pub fn create_segment_key(
        &self,
        master_hash: &str,
        variant_id: u32,
        segment_id: u64,
        filename: &str,
    ) -> HlsCacheKey {
        HlsCacheKey::new(format!(
            "{}/{}/seg_{}_{}",
            master_hash, variant_id, segment_id, filename
        ))
    }

    /// Create a cache key for a playlist.
    pub fn create_playlist_key(&self, master_hash: &str, filename: &str) -> HlsCacheKey {
        HlsCacheKey::new(format!("{}/{}", master_hash, filename))
    }

    /// Create a cache key for an encryption key.
    pub fn create_key_key(
        &self,
        master_hash: &str,
        variant_id: u32,
        filename: &str,
    ) -> HlsCacheKey {
        HlsCacheKey::new(format!("{}/{}/{}", master_hash, variant_id, filename))
    }

    /// Store a resource in cache.
    pub fn cache_resource(&self, key: &HlsCacheKey, data: Bytes) -> io::Result<()> {
        self.cache.put(key, data)
    }

    /// Get a resource from cache.
    pub fn get_cached_resource(&self, key: &HlsCacheKey) -> io::Result<Option<Bytes>> {
        self.cache.get(key)
    }

    /// Check if resource is cached.
    pub fn is_cached(&self, key: &HlsCacheKey) -> io::Result<bool> {
        self.cache.exists(key)
    }
}

impl StorageProvider for SegmentedStorageProvider {
    type Reader = SegmentedReader;
    type Writer = SegmentedWriter;

    fn into_reader_writer(
        self,
        _content_length: Option<u64>,
    ) -> io::Result<(Self::Reader, Self::Writer)> {
        let reader = SegmentedReader::new(self.state.clone(), self.cache.clone());
        let writer = SegmentedWriter::new(self.state.clone(), self.cache.clone());

        Ok((reader, writer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_segmented_storage_basic() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let master_hash = "test_master";

        // Create cache
        let cache = HlsStorageProvider::new(temp_dir.path(), master_hash)?;

        // Create segmented provider
        let provider = SegmentedStorageProvider::new(cache);

        // Test cache operations
        let playlist_key = provider.create_playlist_key(master_hash, "master.m3u8");
        let playlist_data = Bytes::from_static(b"#EXTM3U\n#EXT-X-VERSION:3");

        provider.cache_resource(&playlist_key, playlist_data.clone())?;

        assert!(provider.is_cached(&playlist_key)?);

        let retrieved = provider.get_cached_resource(&playlist_key)?.unwrap();
        assert_eq!(retrieved, playlist_data);

        Ok(())
    }

    #[test]
    fn test_segmented_storage_segments() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let master_hash = "test_master";

        // Create cache
        let cache = HlsStorageProvider::new(temp_dir.path(), master_hash)?;

        // Create segmented provider
        let provider = SegmentedStorageProvider::new(cache);

        // Cache a segment
        let segment_key = provider.create_segment_key(master_hash, 0, 1, "segment.ts");
        let segment_data = Bytes::from_static(b"segment data");

        provider.cache_resource(&segment_key, segment_data.clone())?;

        assert!(provider.is_cached(&segment_key)?);

        let retrieved = provider.get_cached_resource(&segment_key)?.unwrap();
        assert_eq!(retrieved, segment_data);

        Ok(())
    }

    #[test]
    fn test_segmented_reader_writer() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let master_hash = "test_master";

        // Create cache
        let cache = HlsStorageProvider::new(temp_dir.path(), master_hash)?;

        // Create segmented provider
        let provider = SegmentedStorageProvider::new(cache);

        // Get reader/writer
        let (mut reader, mut writer) = provider.into_reader_writer(None)?;

        // First, cache a segment
        let segment_key = provider.create_segment_key(master_hash, 0, 0, "segment0.ts");
        let segment_data = Bytes::from_static(b"segment 0 data");
        provider.cache_resource(&segment_key, segment_data.clone())?;

        // Update state to point to this segment
        {
            let mut state = provider.state.lock().unwrap();
            state
                .segments
                .insert(0, PathBuf::from(segment_key.as_str()));
        }

        // Try to read from cached segment
        let mut buf = [0u8; 10];
        let bytes_read = reader.read(&mut buf)?;

        assert_eq!(bytes_read, 10);
        assert_eq!(&buf[..10], b"segment 0");

        // Test seeking
        reader.seek(io::SeekFrom::Start(8))?;

        let mut buf2 = [0u8; 6];
        let bytes_read2 = reader.read(&mut buf2)?;

        assert_eq!(bytes_read2, 6);
        assert_eq!(&buf2[..6], b"0 data");

        // Test writer (writing to new segment)
        writer.write_all(b"new segment data")?;

        // Finalize the segment
        let new_segment_key = provider.create_segment_key(master_hash, 0, 1, "segment1.ts");
        writer.finalize_current_segment(&new_segment_key)?;

        // Verify segment was cached
        assert!(provider.is_cached(&new_segment_key)?);

        let cached_data = provider.get_cached_resource(&new_segment_key)?.unwrap();
        assert_eq!(cached_data, Bytes::from_static(b"new segment data"));

        Ok(())
    }
}
