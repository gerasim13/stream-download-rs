//! Tests for tree-structured cache implementation

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use stream_download_storage_ext::{
    FileStorageProvider, LeaseAwareCacheTree, StorageBackedBlobCache, TreeStorageFactory,
};
use tempfile::tempdir;

// Simple key type for testing
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TestKey {
    id: u32,
    name: String,
}

// Shared state for tests
struct TestState {
    temp_dir: tempfile::TempDir,
}

impl TestState {
    fn new() -> io::Result<Self> {
        Ok(Self {
            temp_dir: tempdir()?,
        })
    }

    fn create_cache_for_key(
        &self,
        key: &TestKey,
    ) -> io::Result<Option<StorageBackedBlobCache<FileStorageProvider>>> {
        let path = self
            .temp_dir
            .path()
            .join(format!("{}_{}.bin", key.id, key.name));

        // Create FileStorageProvider for this path
        let provider = FileStorageProvider::open(&path)?;

        // Create StorageBackedBlobCache
        StorageBackedBlobCache::new(provider).map(Some)
    }
}

#[test]
fn test_tree_cache_basic_operations() -> io::Result<()> {
    // Create shared test state
    let state = Arc::new(Mutex::new(TestState::new()?));

    // Create a factory that uses the shared state
    let factory = TreeStorageFactory::new(
        move |key: &TestKey| {
            let state = state.lock().unwrap();
            state.create_cache_for_key(key)
        },
        "/cache/root",
    );

    // Create temp directory for lease file
    let temp_dir = tempdir()?;
    let lease_path = temp_dir.path().join("lease.lock");

    // Create the cache tree
    let cache_tree = LeaseAwareCacheTree::new(factory, "/cache/root".into(), lease_path);

    // Test key
    let key1 = TestKey {
        id: 1,
        name: "test1".to_string(),
    };

    // Initially should not exist
    assert!(!cache_tree.exists(&key1)?);
    assert_eq!(cache_tree.len(&key1)?, None);
    assert_eq!(cache_tree.get(&key1)?, None);

    // Put data
    let data1 = Bytes::from("Hello, World!");
    cache_tree.put(&key1, data1.clone())?;

    // Verify data exists
    assert!(cache_tree.exists(&key1)?);
    assert_eq!(cache_tree.len(&key1)?, Some(data1.len() as u64));
    assert_eq!(cache_tree.get(&key1)?, Some(data1.clone()));

    // Test with another key
    let key2 = TestKey {
        id: 2,
        name: "test2".to_string(),
    };

    let data2 = Bytes::from("Another test");
    cache_tree.put(&key2, data2.clone())?;

    assert!(cache_tree.exists(&key2)?);
    assert_eq!(cache_tree.get(&key2)?, Some(data2.clone()));

    // Original key should still have its data
    assert!(cache_tree.exists(&key1)?);
    assert_eq!(cache_tree.get(&key1)?, Some(data1));

    Ok(())
}

#[test]
fn test_tree_cache_overwrite() -> io::Result<()> {
    // Create shared test state
    let state = Arc::new(Mutex::new(TestState::new()?));

    // Create a factory that uses the shared state
    let factory = TreeStorageFactory::new(
        move |key: &TestKey| {
            let state = state.lock().unwrap();
            state.create_cache_for_key(key)
        },
        "/cache/root",
    );

    // Create temp directory for lease file
    let temp_dir = tempdir()?;
    let lease_path = temp_dir.path().join("lease.lock");

    let cache_tree = LeaseAwareCacheTree::new(factory, "/cache/root".into(), lease_path);

    let key = TestKey {
        id: 42,
        name: "overwrite_test".to_string(),
    };

    // Put initial data
    let initial_data = Bytes::from("Initial data");
    cache_tree.put(&key, initial_data.clone())?;
    assert_eq!(cache_tree.get(&key)?, Some(initial_data));

    // Overwrite with new data
    let new_data = Bytes::from("New data");
    cache_tree.put(&key, new_data.clone())?;

    // Should have new data
    assert_eq!(cache_tree.get(&key)?, Some(new_data));

    Ok(())
}

#[test]
fn test_tree_cache_empty_data() -> io::Result<()> {
    // Create shared test state
    let state = Arc::new(Mutex::new(TestState::new()?));

    // Create a factory that uses the shared state
    let factory = TreeStorageFactory::new(
        move |key: &TestKey| {
            let state = state.lock().unwrap();
            state.create_cache_for_key(key)
        },
        "/cache/root",
    );

    // Create temp directory for lease file
    let temp_dir = tempdir()?;
    let lease_path = temp_dir.path().join("lease.lock");

    let cache_tree = LeaseAwareCacheTree::new(factory, "/cache/root".into(), lease_path);

    let key = TestKey {
        id: 99,
        name: "empty_test".to_string(),
    };

    // Put empty data
    let empty_data = Bytes::from("");
    cache_tree.put(&key, empty_data)?;

    // Empty cache should be treated as miss
    assert!(!cache_tree.exists(&key)?);
    assert_eq!(cache_tree.len(&key)?, None);
    assert_eq!(cache_tree.get(&key)?, None);

    Ok(())
}

// Test with a factory that returns None for some keys
fn selective_factory(
    key: &TestKey,
) -> io::Result<Option<StorageBackedBlobCache<FileStorageProvider>>> {
    // Only create cache for even IDs
    if key.id % 2 == 0 {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().join(format!("{}_{}.bin", key.id, key.name));
        let provider = FileStorageProvider::open(&path)?;
        StorageBackedBlobCache::new(provider).map(Some)
    } else {
        Ok(None)
    }
}

#[test]
fn test_tree_cache_selective_factory() -> io::Result<()> {
    let factory = TreeStorageFactory::new(selective_factory, "/cache/root");

    // Create temp directory for lease file
    let temp_dir = tempdir()?;
    let lease_path = temp_dir.path().join("lease.lock");

    let cache_tree = LeaseAwareCacheTree::new(factory, "/cache/root".into(), lease_path);

    // Even ID - should work
    let even_key = TestKey {
        id: 2,
        name: "even".to_string(),
    };

    let data = Bytes::from("Even data");
    cache_tree.put(&even_key, data.clone())?;
    assert!(cache_tree.exists(&even_key)?);
    assert_eq!(cache_tree.get(&even_key)?, Some(data));

    // Odd ID - factory returns None, so operations should be no-ops
    let odd_key = TestKey {
        id: 1,
        name: "odd".to_string(),
    };

    // These should not panic, just return default values
    assert!(!cache_tree.exists(&odd_key)?);
    assert_eq!(cache_tree.len(&odd_key)?, None);
    assert_eq!(cache_tree.get(&odd_key)?, None);

    // Put should also be a no-op
    cache_tree.put(&odd_key, Bytes::from("Should not be stored"))?;
    assert!(!cache_tree.exists(&odd_key)?);
    assert_eq!(cache_tree.get(&odd_key)?, None);

    Ok(())
}
