//! Integration tests for stream-download-cache
//!
//! These tests verify that the cache works correctly with core storage providers
//! from the stream-download crate.

use std::io;

use bytes::Bytes;
use stream_download::storage::memory::MemoryStorageProvider;
use stream_download_cache::{BlobCache, StorageBackedBlobCache};

#[test]
fn test_memory_storage_provider_integration() -> io::Result<()> {
    let provider = MemoryStorageProvider;
    let cache = StorageBackedBlobCache::new(provider)?;
    
    // Test put/get roundtrip
    let original_data = Bytes::from("test data for memory storage");
    cache.put(original_data.clone())?;
    
    let retrieved_data = cache.get()?.expect("data should exist");
    assert_eq!(retrieved_data, original_data);
    
    // Test metadata
    assert!(cache.exists()?);
    assert_eq!(cache.len()?, Some(original_data.len() as u64));
    
    Ok(())
}

#[test]
fn test_memory_storage_overwrite() -> io::Result<()> {
    let provider = MemoryStorageProvider;
    let cache = StorageBackedBlobCache::new(provider)?;
    
    // Put initial data
    let initial = Bytes::from("initial content");
    cache.put(initial.clone())?;
    assert_eq!(cache.get()?, Some(initial));
    
    // Overwrite with different data
    let overwrite = Bytes::from("overwrite content");
    cache.put(overwrite.clone())?;
    
    // Verify overwrite
    assert_eq!(cache.get()?, Some(overwrite.clone()));
    assert_eq!(cache.len()?, Some(overwrite.len() as u64));
    
    Ok(())
}

#[test]
fn test_memory_storage_empty_cache() -> io::Result<()> {
    let provider = MemoryStorageProvider;
    let cache = StorageBackedBlobCache::new(provider)?;
    
    // Initially empty
    assert!(!cache.exists()?);
    assert_eq!(cache.len()?, None);
    assert_eq!(cache.get()?, None);
    
    // Put empty data
    cache.put(Bytes::from(""))?;
    
    // Empty data should be treated as miss
    assert!(!cache.exists()?);
    assert_eq!(cache.len()?, None);
    assert_eq!(cache.get()?, None);
    
    Ok(())
}

#[test]
fn test_memory_storage_large_data() -> io::Result<()> {
    let provider = MemoryStorageProvider;
    let cache = StorageBackedBlobCache::new(provider)?;
    
    // Test with larger data (1MB)
    let large_data = Bytes::from(vec![0u8; 1024 * 1024]);
    cache.put(large_data.clone())?;
    
    let retrieved = cache.get()?.expect("large data should exist");
    assert_eq!(retrieved.len(), large_data.len());
    assert_eq!(retrieved, large_data);
    
    Ok(())
}

#[cfg(feature = "temp-storage")]
mod temp_storage_tests {
    use super::*;
    use stream_download::storage::temp::TempStorageProvider;
    use stream_download::storage::temp::tempfile;

    #[test]
    fn test_temp_storage_provider_integration() -> io::Result<()> {
        let provider = TempStorageProvider::with_tempfile_builder(|| {
            tempfile::Builder::new().suffix("cache_test").tempfile()
        });
        let cache = StorageBackedBlobCache::new(provider)?;
        
        // Test put/get roundtrip
        let original_data = Bytes::from("test data for temp storage");
        cache.put(original_data.clone())?;
        
        let retrieved_data = cache.get()?.expect("data should exist");
        assert_eq!(retrieved_data, original_data);
        
        // Test metadata
        assert!(cache.exists()?);
        assert_eq!(cache.len()?, Some(original_data.len() as u64));
        
        Ok(())
    }
    
    #[test]
    fn test_temp_storage_persistence() -> io::Result<()> {
        let provider = TempStorageProvider::with_tempfile_builder(|| {
            tempfile::Builder::new().suffix("cache_persist").tempfile()
        });
        let cache = StorageBackedBlobCache::new(provider)?;
        
        // Put data
        let data = Bytes::from("persistent data");
        cache.put(data.clone())?;
        
        // Verify it exists
        assert!(cache.exists()?);
        assert_eq!(cache.get()?, Some(data));
        
        Ok(())
    }
}