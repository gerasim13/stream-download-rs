//! Storage-backed cache implementations for stream-download.
//!
//! This crate provides cache implementations that work exclusively through
//! the `StorageProvider` interface from `stream-download`, without any direct
//! filesystem access. All cached data is stored as `bytes::Bytes` for efficient
//! memory management and zero-copy operations where possible.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::RwLock;

use stream_download::storage::StorageProvider;

/// Trait for caching a single blob of data.
///
/// This is a simple single-object cache that can store, retrieve, and query
/// one blob of bytes at a time. It's designed to work over any `StorageProvider`
/// implementation without requiring direct filesystem access.
pub trait BlobCache: Send + Sync {
    /// Get the cached blob, if present.
    ///
    /// Returns `Ok(None)` if the cache is empty or the object doesn't exist.
    fn get(&self) -> io::Result<Option<Bytes>>;

    /// Store a blob in the cache, overwriting any existing data.
    fn put(&self, data: Bytes) -> io::Result<()>;

    /// Get the length of the cached blob, if present.
    ///
    /// Returns `Ok(None)` if the cache is empty or the object doesn't exist.
    fn len(&self) -> io::Result<Option<u64>>;

    /// Check if the cache contains a blob.
    ///
    /// Returns `Ok(true)` if a blob exists, `Ok(false)` if the cache is empty.
    fn exists(&self) -> io::Result<bool>;
}

/// A `BlobCache` implementation backed by a `StorageProvider`.
///
/// This cache stores data using the provided `StorageProvider` by consuming
/// it and using the resulting `StorageReader` and `StorageWriter`. The cache
/// treats the storage as a single object that can be overwritten.
///
/// # Design Constraints
///
/// - Works with any `StorageProvider` implementation
/// - No direct filesystem access
/// - Uses `Bytes` for all data operations
/// - Single-object cache (no key-value semantics)
///
/// # Semantics
///
/// - Empty cache (length 0) is treated as a miss
/// - `put` operations overwrite the entire object
/// - All operations are thread-safe
pub struct StorageBackedBlobCache<P>
where
    P: StorageProvider,
{
    reader: Arc<RwLock<P::Reader>>,
    writer: Arc<RwLock<P::Writer>>,
    actual_len: AtomicU64,
}

impl<P> StorageBackedBlobCache<P>
where
    P: StorageProvider,
{
    /// Create a new cache backed by the given storage provider.
    ///
    /// This consumes the storage provider and creates a reader/writer pair.
    /// The cache will use the storage as a single object that can be overwritten.
    pub fn new(provider: P) -> io::Result<Self> {
        let (reader, writer) = provider.into_reader_writer(None)?;
        Ok(Self {
            reader: Arc::new(RwLock::new(reader)),
            writer: Arc::new(RwLock::new(writer)),
            actual_len: AtomicU64::new(0),
        })
    }
}

impl<P> BlobCache for StorageBackedBlobCache<P>
where
    P: StorageProvider,
    P::Reader: Sync + Send,
    P::Writer: Sync + Send,
{
    fn get(&self) -> io::Result<Option<Bytes>> {
        let actual_len = self.actual_len.load(Ordering::SeqCst);
        
        // Empty cache is treated as a miss
        if actual_len == 0 {
            return Ok(None);
        }
        
        let mut reader = self.reader.write();
        
        // Store current position
        let current_pos = reader.seek(SeekFrom::Current(0))?;
        
        // Read the actual content length
        reader.seek(SeekFrom::Start(0))?;
        let mut vec_buffer = vec![0u8; actual_len as usize];
        reader.read_exact(&mut vec_buffer)?;
        
        // Restore original position
        reader.seek(SeekFrom::Start(current_pos))?;
        
        Ok(Some(Bytes::from(vec_buffer)))
    }

    fn put(&self, data: Bytes) -> io::Result<()> {
        let mut writer = self.writer.write();
        
        // Overwrite the entire object
        writer.seek(SeekFrom::Start(0))?;
        writer.write_all(&data)?;
        writer.flush()?;
        
        // Update the actual data length
        self.actual_len.store(data.len() as u64, Ordering::SeqCst);
        
        Ok(())
    }

    fn len(&self) -> io::Result<Option<u64>> {
        let len = self.actual_len.load(Ordering::SeqCst);
        if len == 0 {
            Ok(None)
        } else {
            Ok(Some(len))
        }
    }

    fn exists(&self) -> io::Result<bool> {
        let len = self.actual_len.load(Ordering::SeqCst);
        Ok(len > 0)
    }
}

/// Type alias for a shared blob cache handle.
pub type SharedBlobCache = Arc<dyn BlobCache>;

#[cfg(test)]
mod tests {
    use super::*;
    use stream_download::storage::memory::MemoryStorageProvider;

    #[test]
    fn test_memory_cache_roundtrip() -> io::Result<()> {
        let provider = MemoryStorageProvider;
        let cache = StorageBackedBlobCache::new(provider)?;
        
        // Initially empty
        assert!(!cache.exists()?);
        assert_eq!(cache.len()?, None);
        assert_eq!(cache.get()?, None);
        
        // Put data
        let data = Bytes::from("hello world");
        cache.put(data.clone())?;
        
        // Verify data exists
        assert!(cache.exists()?);
        assert_eq!(cache.len()?, Some(data.len() as u64));
        assert_eq!(cache.get()?, Some(data));
        
        Ok(())
    }

    #[test]
    fn test_memory_cache_overwrite() -> io::Result<()> {
        let provider = MemoryStorageProvider;
        let cache = StorageBackedBlobCache::new(provider)?;
        
        // Put initial data
        let initial_data = Bytes::from("initial data");
        cache.put(initial_data.clone())?;
        assert_eq!(cache.get()?, Some(initial_data));
        
        // Overwrite with new data
        let new_data = Bytes::from("new data");
        cache.put(new_data.clone())?;
        
        // Verify new data
        assert_eq!(cache.len()?, Some(new_data.len() as u64));
        assert_eq!(cache.get()?, Some(new_data));
        
        Ok(())
    }

    #[test]
    fn test_memory_cache_empty() -> io::Result<()> {
        let provider = MemoryStorageProvider;
        let cache = StorageBackedBlobCache::new(provider)?;
        
        // Put empty data
        let empty_data = Bytes::from("");
        cache.put(empty_data)?;
        
        // Empty cache should be treated as miss
        assert!(!cache.exists()?);
        assert_eq!(cache.len()?, None);
        assert_eq!(cache.get()?, None);
        
        Ok(())
    }
}