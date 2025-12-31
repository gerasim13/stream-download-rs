//! Tests for tree-structured cache implementation

use std::io;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use stream_download::storage::memory::MemoryStorageProvider;
use stream_download_storage_ext::{
    BlobCache, CacheKVTree, StorageBackedBlobCache, TreeStorageFactory,
};

// Simple key type for testing
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TestKey {
    id: u32,
    name: String,
}

// Factory function that creates a StorageBackedBlobCache for a given key
fn create_cache_for_key(
    key: &TestKey,
) -> io::Result<Option<StorageBackedBlobCache<MemoryStorageProvider>>> {
    // In a real implementation, you might create different storage providers
    // based on the key, or use a shared provider with different configurations
    let provider = MemoryStorageProvider;
    StorageBackedBlobCache::new(provider).map(Some)
}

#[test]
fn test_tree_cache_basic_operations() -> io::Result<()> {
    // Create a factory
    let factory = TreeStorageFactory::new(create_cache_for_key, "/cache/root");

    // Create inner storage provider
    let inner_provider = Arc::new(Mutex::new(MemoryStorageProvider));

    // Create the cache tree
    let cache_tree = CacheKVTree::new(factory, inner_provider, "/cache/root".into());

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
    let factory = TreeStorageFactory::new(create_cache_for_key, "/cache/root");
    let inner_provider = Arc::new(Mutex::new(MemoryStorageProvider));
    let cache_tree = CacheKVTree::new(factory, inner_provider, "/cache/root".into());

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
    let factory = TreeStorageFactory::new(create_cache_for_key, "/cache/root");
    let inner_provider = Arc::new(Mutex::new(MemoryStorageProvider));
    let cache_tree = CacheKVTree::new(factory, inner_provider, "/cache/root".into());

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

#[test]
fn test_tree_cache_concurrent_access() -> io::Result<()> {
    use std::thread;

    let factory = TreeStorageFactory::new(create_cache_for_key, "/cache/root");
    let inner_provider = Arc::new(Mutex::new(MemoryStorageProvider));
    let cache_tree = Arc::new(CacheKVTree::new(
        factory,
        inner_provider,
        "/cache/root".into(),
    ));

    let mut handles = vec![];

    // Spawn multiple threads to test concurrent access
    for i in 0..5 {
        let cache_clone = cache_tree.clone();
        let handle = thread::spawn(move || -> io::Result<()> {
            let key = TestKey {
                id: i,
                name: format!("thread_{}", i),
            };

            let data = Bytes::from(format!("Data from thread {}", i));
            cache_clone.put(&key, data.clone())?;

            // Verify in the same thread
            assert!(cache_clone.exists(&key)?);
            assert_eq!(cache_clone.get(&key)?, Some(data));

            Ok(())
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread panicked")?;
    }

    // Verify all keys exist in main thread
    for i in 0..5 {
        let key = TestKey {
            id: i,
            name: format!("thread_{}", i),
        };

        assert!(cache_tree.exists(&key)?);
        let expected_data = format!("Data from thread {}", i);
        assert_eq!(cache_tree.get(&key)?, Some(Bytes::from(expected_data)));
    }

    Ok(())
}

// Test with a factory that returns None for some keys
fn selective_factory(
    key: &TestKey,
) -> io::Result<Option<StorageBackedBlobCache<MemoryStorageProvider>>> {
    // Only create cache for even IDs
    if key.id % 2 == 0 {
        let provider = MemoryStorageProvider;
        StorageBackedBlobCache::new(provider).map(Some)
    } else {
        Ok(None)
    }
}

#[test]
fn test_tree_cache_selective_factory() -> io::Result<()> {
    let factory = TreeStorageFactory::new(selective_factory, "/cache/root");
    let inner_provider = Arc::new(Mutex::new(MemoryStorageProvider));
    let cache_tree = CacheKVTree::new(factory, inner_provider, "/cache/root".into());

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
