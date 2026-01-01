//! Tests for LeaseAwareCacheTree public API

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use stream_download_storage_ext::{
    Lease, LeaseAwareCacheTree, LeaseAwareStorageProvider, StorageBackedBlobCache,
    TreeStorageFactory,
};
use tempfile::tempdir;

// Simple key type for testing
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TestKey {
    id: u32,
    name: String,
}

// Factory function that creates a StorageBackedBlobCache for a given key
fn create_cache_for_key(
    key: &TestKey,
) -> io::Result<Option<StorageBackedBlobCache<stream_download_storage_ext::FileStorageProvider>>> {
    // Create a unique path for each key in a temp directory
    let temp_dir = tempdir()?;
    let path = temp_dir.path().join(format!("{}_{}.bin", key.id, key.name));

    // Create FileStorageProvider for this path
    let provider = stream_download_storage_ext::FileStorageProvider::open(&path)?;

    // Create StorageBackedBlobCache
    StorageBackedBlobCache::new(provider).map(Some)
}

#[test]
fn test_lease_aware_cache_basic_operations() -> io::Result<()> {
    // Create a factory
    let factory = TreeStorageFactory::new(create_cache_for_key, "/cache/root");

    // Create temp directory for lease file
    let temp_dir = tempdir()?;
    let lease_path = temp_dir.path().join("lease.lock");

    // Create the lease-aware cache tree
    let cache = LeaseAwareCacheTree::new(factory, "/cache/root".into(), lease_path.clone());

    // Test key
    let key1 = TestKey {
        id: 1,
        name: "test1".to_string(),
    };

    // Initially should not exist
    assert!(!cache.exists(&key1)?);
    assert_eq!(cache.len(&key1)?, None);
    assert_eq!(cache.get(&key1)?, None);

    // Put data
    let data1 = Bytes::from("Hello, World!");
    cache.put(&key1, data1.clone())?;

    // Verify data exists
    assert!(cache.exists(&key1)?);
    assert_eq!(cache.len(&key1)?, Some(data1.len() as u64));
    assert_eq!(cache.get(&key1)?, Some(data1.clone()));

    // Test with another key
    let key2 = TestKey {
        id: 2,
        name: "test2".to_string(),
    };

    let data2 = Bytes::from("Another test");
    cache.put(&key2, data2.clone())?;

    assert!(cache.exists(&key2)?);
    assert_eq!(cache.get(&key2)?, Some(data2.clone()));

    // Original key should still have its data
    assert!(cache.exists(&key1)?);
    assert_eq!(cache.get(&key1)?, Some(data1));

    Ok(())
}

#[test]
fn test_lease_aware_cache_lease_operations() -> io::Result<()> {
    let factory = TreeStorageFactory::new(create_cache_for_key, "/cache/root");

    // Create temp directory for lease file
    let temp_dir = tempdir()?;
    let lease_path = temp_dir.path().join("lease.lock");

    let cache = LeaseAwareCacheTree::new(factory, "/cache/root".into(), lease_path.clone());

    // Initially lease file should not exist
    assert!(!Lease::exists(&cache));

    // Touch the lease (create/update it)
    cache.touch()?;

    // Now lease should exist
    assert!(Lease::exists(&cache));

    // Age should be very small (just created)
    let age = cache.age();
    assert!(age < Duration::from_secs(1));

    // Should be occupied (age < TTL)
    assert!(LeaseAwareStorageProvider::occupied(&cache));

    // Remove the lease
    cache.remove()?;

    // Lease should not exist anymore
    assert!(!Lease::exists(&cache));

    Ok(())
}

#[test]
fn test_lease_aware_cache_maybe_evict() -> io::Result<()> {
    let factory = TreeStorageFactory::new(create_cache_for_key, "/cache/root");

    // Create temp directory for lease file
    let temp_dir = tempdir()?;
    let lease_path = temp_dir.path().join("lease.lock");

    let cache = LeaseAwareCacheTree::new(factory, "/cache/root".into(), lease_path.clone());

    // Create a key and store data
    let key = TestKey {
        id: 42,
        name: "evict_test".to_string(),
    };

    let data = Bytes::from("Test data for eviction");
    cache.put(&key, data.clone())?;

    // Initially no lease, so maybe_evict should remove nothing
    assert!(!Lease::exists(&cache));
    LeaseAwareStorageProvider::maybe_evict(&cache)?;
    // Data should still exist
    assert!(cache.exists(&key)?);

    // Create lease
    cache.touch()?;
    assert!(Lease::exists(&cache));
    assert!(LeaseAwareStorageProvider::occupied(&cache));

    // maybe_evict should not remove since it's occupied
    LeaseAwareStorageProvider::maybe_evict(&cache)?;
    assert!(Lease::exists(&cache));
    assert!(cache.exists(&key)?);

    Ok(())
}

#[test]
fn test_lease_aware_cache_overwrite() -> io::Result<()> {
    let factory = TreeStorageFactory::new(create_cache_for_key, "/cache/root");

    // Create temp directory for lease file
    let temp_dir = tempdir()?;
    let lease_path = temp_dir.path().join("lease.lock");

    let cache = LeaseAwareCacheTree::new(factory, "/cache/root".into(), lease_path.clone());

    let key = TestKey {
        id: 42,
        name: "overwrite_test".to_string(),
    };

    // Put initial data
    let initial_data = Bytes::from("Initial data");
    cache.put(&key, initial_data.clone())?;
    assert_eq!(cache.get(&key)?, Some(initial_data));

    // Overwrite with new data
    let new_data = Bytes::from("New data");
    cache.put(&key, new_data.clone())?;

    // Should have new data
    assert_eq!(cache.get(&key)?, Some(new_data));

    Ok(())
}

#[test]
fn test_lease_aware_cache_empty_data() -> io::Result<()> {
    let factory = TreeStorageFactory::new(create_cache_for_key, "/cache/root");

    // Create temp directory for lease file
    let temp_dir = tempdir()?;
    let lease_path = temp_dir.path().join("lease.lock");

    let cache = LeaseAwareCacheTree::new(factory, "/cache/root".into(), lease_path.clone());

    let key = TestKey {
        id: 99,
        name: "empty_test".to_string(),
    };

    // Put empty data
    let empty_data = Bytes::from("");
    cache.put(&key, empty_data)?;

    // Empty cache should be treated as miss
    assert!(!cache.exists(&key)?);
    assert_eq!(cache.len(&key)?, None);
    assert_eq!(cache.get(&key)?, None);

    Ok(())
}

// Test with a factory that returns None for some keys
fn selective_factory(
    key: &TestKey,
) -> io::Result<Option<StorageBackedBlobCache<stream_download_storage_ext::FileStorageProvider>>> {
    // Only create cache for even IDs
    if key.id % 2 == 0 {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().join(format!("{}_{}.bin", key.id, key.name));
        let provider = stream_download_storage_ext::FileStorageProvider::open(&path)?;
        StorageBackedBlobCache::new(provider).map(Some)
    } else {
        Ok(None)
    }
}

#[test]
fn test_lease_aware_cache_selective_factory() -> io::Result<()> {
    let factory = TreeStorageFactory::new(selective_factory, "/cache/root");

    // Create temp directory for lease file
    let temp_dir = tempdir()?;
    let lease_path = temp_dir.path().join("lease.lock");

    let cache = LeaseAwareCacheTree::new(factory, "/cache/root".into(), lease_path.clone());

    // Even ID - should work
    let even_key = TestKey {
        id: 2,
        name: "even".to_string(),
    };

    let data = Bytes::from("Even data");
    cache.put(&even_key, data.clone())?;
    assert!(cache.exists(&even_key)?);
    assert_eq!(cache.get(&even_key)?, Some(data));

    // Odd ID - factory returns None, so operations should be no-ops
    let odd_key = TestKey {
        id: 1,
        name: "odd".to_string(),
    };

    // These should not panic, just return default values
    assert!(!cache.exists(&odd_key)?);
    assert_eq!(cache.len(&odd_key)?, None);
    assert_eq!(cache.get(&odd_key)?, None);

    // Put should also be a no-op
    cache.put(&odd_key, Bytes::from("Should not be stored"))?;
    assert!(!cache.exists(&odd_key)?);
    assert_eq!(cache.get(&odd_key)?, None);

    Ok(())
}

#[test]
fn test_lease_aware_cache_ttl() -> io::Result<()> {
    let factory = TreeStorageFactory::new(create_cache_for_key, "/cache/root");

    // Create temp directory for lease file
    let temp_dir = tempdir()?;
    let lease_path = temp_dir.path().join("lease.lock");

    let cache = LeaseAwareCacheTree::new(factory, "/cache/root".into(), lease_path.clone());

    // Create lease
    cache.touch()?;
    assert!(Lease::exists(&cache));
    assert!(LeaseAwareStorageProvider::occupied(&cache));

    // Default TTL should be 6 hours
    assert_eq!(
        <LeaseAwareCacheTree<stream_download_storage_ext::FileStorageProvider, TestKey> as LeaseAwareStorageProvider>::LEASE_TTL,
        Duration::from_secs(6 * 60 * 60)
    );

    Ok(())
}
