//! HLS factory for creating cache providers.
//!
//! This module provides factory functions for creating `StorageBackedBlobCache` instances
//! from `HlsCacheKey` values, suitable for use with `LeaseAwareCacheTree`.

use std::io;
use std::path::PathBuf;

use stream_download_storage_ext::{
    FileStorageProvider, StorageBackedBlobCache, TreeStorageFactory,
};

use super::keys::HlsCacheKey;

/// Create a factory function for HLS cache providers.
///
/// Returns a function that takes an `HlsCacheKey` and returns a `StorageBackedBlobCache<FileStorageProvider>`
/// with the file path constructed as `<storage_root>/<key>`.
pub fn create_hls_factory(
    storage_root: impl Into<PathBuf>,
) -> impl Fn(&HlsCacheKey) -> io::Result<Option<StorageBackedBlobCache<FileStorageProvider>>>
+ Send
+ Sync
+ 'static {
    let storage_root = storage_root.into();

    move |key: &HlsCacheKey| -> io::Result<Option<StorageBackedBlobCache<FileStorageProvider>>> {
        // Convert the key to a filesystem path
        let relative_path = key.to_path();
        let full_path = storage_root.join(relative_path);

        // Ensure parent directory exists
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create FileStorageProvider for this path
        let provider = FileStorageProvider::open(&full_path)?;

        // Create StorageBackedBlobCache
        StorageBackedBlobCache::new(provider).map(Some)
    }
}

/// Create a `TreeStorageFactory` for HLS caching.
///
/// This creates a factory that can be used with `LeaseAwareCacheTree` to cache HLS resources.
pub fn create_hls_tree_factory(
    storage_root: impl Into<PathBuf>,
) -> TreeStorageFactory<FileStorageProvider, HlsCacheKey> {
    let factory_fn = create_hls_factory(storage_root);
    TreeStorageFactory::new(factory_fn, "/")
}

/// Create a lease-aware cache tree for HLS resources.
///
/// This creates a complete `LeaseAwareCacheTree` configured for HLS caching.
pub fn create_hls_cache_tree(
    storage_root: impl Into<PathBuf>,
    master_hash: &str,
) -> io::Result<stream_download_storage_ext::LeaseAwareCacheTree<FileStorageProvider, HlsCacheKey>>
{
    use stream_download_storage_ext::LeaseAwareCacheTree;

    let storage_root = storage_root.into();

    // Create the master directory path
    let master_dir = storage_root.join(master_hash);

    // Ensure master directory exists
    std::fs::create_dir_all(&master_dir)?;

    // Create lease file path
    let lease_path = master_dir.join(".lease");

    // Create factory
    let factory = create_hls_tree_factory(storage_root.clone());

    // Create the cache tree
    let cache_tree = LeaseAwareCacheTree::new(factory, storage_root, lease_path);

    Ok(cache_tree)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_hls_factory() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let factory_fn = create_hls_factory(temp_dir.path());

        let key = HlsCacheKey::new("master_hash/variant_id/filename".to_string());

        // Create cache provider
        let cache = factory_fn(&key)?.expect("Should create cache");

        // Test that we can write and read data
        let test_data = b"test data";
        cache.put(bytes::Bytes::from_static(test_data))?;

        // Read back the data
        let read_data = cache.get()?.expect("Should have data");
        assert_eq!(read_data.as_ref(), test_data);

        // Verify file was created
        let expected_path = temp_dir.path().join("master_hash/variant_id/filename");
        assert!(expected_path.exists());

        Ok(())
    }

    #[test]
    fn test_create_hls_tree_factory() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let factory = create_hls_tree_factory(temp_dir.path());

        let key = HlsCacheKey::new("master_hash/variant_id/filename".to_string());

        // Get node from factory
        let node = factory.node(&key)?.expect("Should create node");

        // Test that we can write and read data
        let test_data = b"test data";
        node.put(bytes::Bytes::from_static(test_data))?;

        // Read back the data
        let read_data = node.get()?.expect("Should have data");
        assert_eq!(read_data.as_ref(), test_data);

        Ok(())
    }

    #[test]
    fn test_create_hls_cache_tree() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let master_hash = "test_master_hash";

        let cache_tree = create_hls_cache_tree(temp_dir.path(), master_hash)?;

        // Test with a key
        let key = HlsCacheKey::new(format!("{}/variant_id/filename", master_hash));

        // Initially should not exist
        assert!(!cache_tree.exists(&key)?);

        // Put data
        let test_data = bytes::Bytes::from_static(b"test data");
        cache_tree.put(&key, test_data.clone())?;

        // Should exist now
        assert!(cache_tree.exists(&key)?);

        // Get data back
        let read_data = cache_tree.get(&key)?.expect("Should have data");
        assert_eq!(read_data, test_data);

        // Verify lease file was created
        let lease_path = temp_dir.path().join(master_hash).join(".lease");
        assert!(lease_path.exists());

        Ok(())
    }

    #[test]
    fn test_hls_cache_key_path_conversion() {
        let key = HlsCacheKey::new("master_hash/variant_id/filename.ts".to_string());

        // Test path conversion
        let path = key.to_path();
        assert_eq!(
            path,
            std::path::PathBuf::from("master_hash/variant_id/filename.ts")
        );

        // Test master hash extraction
        assert_eq!(key.master_hash(), Some("master_hash"));

        // Test as_str
        assert_eq!(key.as_str(), "master_hash/variant_id/filename.ts");

        // Test Display trait
        assert_eq!(key.to_string(), "master_hash/variant_id/filename.ts");
    }

    #[test]
    fn test_hls_cache_key_from_string_and_str() {
        // Test From<String>
        let key1: HlsCacheKey = "master/playlist.m3u8".to_string().into();
        assert_eq!(key1.as_str(), "master/playlist.m3u8");

        // Test From<&str>
        let key2: HlsCacheKey = "master/variant/segment.ts".into();
        assert_eq!(key2.as_str(), "master/variant/segment.ts");
    }

    #[test]
    fn test_hls_factory_with_different_key_formats() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let factory_fn = create_hls_factory(temp_dir.path());

        // Test different HLS key formats
        let test_cases = vec![
            // Playlist format
            "master_hash/playlist.m3u8",
            // Key format
            "master_hash/variant_id/key.bin",
            // Init segment format
            "master_hash/variant_id/init_init.mp4",
            // Media segment format
            "master_hash/variant_id/seg_segment-001.ts",
        ];

        for key_str in test_cases {
            let key = HlsCacheKey::new(key_str.to_string());

            // Create cache provider
            let cache = factory_fn(&key)?.expect("Should create cache");

            // Test write/read
            let test_data = format!("data for {}", key_str);
            cache.put(bytes::Bytes::from(test_data.clone()))?;

            let read_data = cache.get()?.expect("Should have data");
            assert_eq!(read_data, test_data.as_bytes());

            // Verify file was created
            let expected_path = temp_dir.path().join(key_str);
            assert!(
                expected_path.exists(),
                "File should exist: {:?}",
                expected_path
            );
        }

        Ok(())
    }

    #[test]
    fn test_hls_factory_nonexistent_parent_directory() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let factory_fn = create_hls_factory(temp_dir.path());

        // Key with nested directory structure
        let key = HlsCacheKey::new("master_hash/deeply/nested/path/filename.ts".to_string());

        // This should create all parent directories
        let cache = factory_fn(&key)?.expect("Should create cache");

        // Write data
        let test_data = b"test data";
        cache.put(bytes::Bytes::from_static(test_data))?;

        // Verify directories were created
        let expected_path = temp_dir
            .path()
            .join("master_hash/deeply/nested/path/filename.ts");
        assert!(expected_path.exists());

        // Verify parent directories exist
        assert!(
            temp_dir
                .path()
                .join("master_hash/deeply/nested/path")
                .exists()
        );
        assert!(temp_dir.path().join("master_hash/deeply/nested").exists());
        assert!(temp_dir.path().join("master_hash/deeply").exists());
        assert!(temp_dir.path().join("master_hash").exists());

        Ok(())
    }

    #[test]
    fn test_hls_cache_tree_lease_operations() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let master_hash = "test_master_hash";

        let cache_tree = create_hls_cache_tree(temp_dir.path(), master_hash)?;

        // Initially lease should not exist
        assert!(!cache_tree.exists());

        // Touch the lease
        cache_tree.touch()?;
        assert!(cache_tree.exists());

        // Check age (should be very recent)
        let age = cache_tree.age();
        assert!(age < std::time::Duration::from_secs(1));

        // Should be occupied (age < TTL)
        assert!(cache_tree.occupied());

        // Remove lease
        cache_tree.remove()?;
        assert!(!cache_tree.exists());

        Ok(())
    }

    #[test]
    fn test_hls_cache_tree_multiple_keys() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let master_hash = "test_master_hash";

        let cache_tree = create_hls_cache_tree(temp_dir.path(), master_hash)?;

        // Test multiple keys
        let keys = vec![
            HlsCacheKey::new(format!("{}/playlist.m3u8", master_hash)),
            HlsCacheKey::new(format!("{}/0/key.bin", master_hash)),
            HlsCacheKey::new(format!("{}/0/seg_segment-001.ts", master_hash)),
            HlsCacheKey::new(format!("{}/1/seg_segment-002.ts", master_hash)),
        ];

        // Store data for each key
        for (i, key) in keys.iter().enumerate() {
            let test_data = format!("data {}", i);
            cache_tree.put(key, bytes::Bytes::from(test_data.clone()))?;

            // Verify data was stored
            let read_data = cache_tree.get(key)?.expect("Should have data");
            assert_eq!(read_data, test_data.as_bytes());

            // Verify file was created
            let expected_path = temp_dir.path().join(key.as_str());
            assert!(
                expected_path.exists(),
                "File should exist: {:?}",
                expected_path
            );
        }

        // Verify all keys exist
        for key in &keys {
            assert!(cache_tree.exists(key)?);
            assert!(cache_tree.len(key)?.is_some());
        }

        Ok(())
    }
}
