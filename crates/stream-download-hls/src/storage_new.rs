//! New HLS storage provider based on stream-download-storage-ext.
//!
//! This module provides a modern HLS storage implementation that uses
//! LeaseAwareCacheTree for caching and lease management.

use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

use stream_download::storage::StorageProvider;
use stream_download_storage_ext::{
    FileStorageProvider, Lease, LeaseAwareCacheTree, LeaseAwareStorageProvider,
    StorageBackedBlobCache, TreeStorageFactory,
};

use crate::cache::keys::HlsCacheKey;

/// Factory for creating HLS cache providers.
#[derive(Clone)]
pub struct HlsCacheFactory {
    storage_root: PathBuf,
}

impl HlsCacheFactory {
    /// Create a new HLS cache factory.
    pub fn new(storage_root: impl Into<PathBuf>) -> Self {
        Self {
            storage_root: storage_root.into(),
        }
    }

    /// Get the storage root path.
    pub fn storage_root(&self) -> &Path {
        &self.storage_root
    }

    /// Create a cache provider for a specific HLS key.
    pub fn create_cache_provider(
        &self,
        key: &HlsCacheKey,
    ) -> io::Result<StorageBackedBlobCache<FileStorageProvider>> {
        // Convert the key to a filesystem path
        let relative_path = key.to_path();
        let full_path = self.storage_root.join(relative_path);

        // Ensure parent directory exists
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create FileStorageProvider for this path
        let provider = FileStorageProvider::open(&full_path)?;

        // Create StorageBackedBlobCache
        StorageBackedBlobCache::new(provider)
    }

    /// Create a TreeStorageFactory for use with LeaseAwareCacheTree.
    pub fn create_tree_factory(&self) -> TreeStorageFactory<FileStorageProvider, HlsCacheKey> {
        let storage_root = self.storage_root.clone();
        let factory_fn = move |key: &HlsCacheKey| -> io::Result<
            Option<StorageBackedBlobCache<FileStorageProvider>>,
        > {
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
        };

        TreeStorageFactory::new(factory_fn, "/")
    }
}

/// HLS storage provider that caches resources using LeaseAwareCacheTree.
#[derive(Debug)]
pub struct HlsStorageProvider {
    cache_tree: LeaseAwareCacheTree<FileStorageProvider, HlsCacheKey>,
    master_hash: String,
}

impl HlsStorageProvider {
    /// Create a new HLS storage provider.
    pub fn new(
        storage_root: impl Into<PathBuf>,
        master_hash: impl Into<String>,
    ) -> io::Result<Self> {
        let storage_root = storage_root.into();
        let master_hash = master_hash.into();

        // Create the master directory path
        let master_dir = storage_root.join(&master_hash);

        // Ensure master directory exists
        std::fs::create_dir_all(&master_dir)?;

        // Create lease file path
        let lease_path = master_dir.join(".lease");

        // Create factory
        let factory = HlsCacheFactory::new(storage_root.clone()).create_tree_factory();

        // Create the cache tree
        let cache_tree = LeaseAwareCacheTree::new(factory, storage_root, lease_path);

        Ok(Self {
            cache_tree,
            master_hash,
        })
    }

    /// Get the master hash.
    pub fn master_hash(&self) -> &str {
        &self.master_hash
    }

    /// Get a cached resource.
    pub fn get(&self, key: &HlsCacheKey) -> io::Result<Option<bytes::Bytes>> {
        self.cache_tree.get(key)
    }

    /// Store a resource in the cache.
    pub fn put(&self, key: &HlsCacheKey, data: bytes::Bytes) -> io::Result<()> {
        self.cache_tree.put(key, data)
    }

    /// Check if a resource exists in the cache.
    pub fn exists(&self, key: &HlsCacheKey) -> io::Result<bool> {
        self.cache_tree.exists(key)
    }

    /// Get the length of a cached resource.
    pub fn len(&self, key: &HlsCacheKey) -> io::Result<Option<u64>> {
        self.cache_tree.len(key)
    }

    /// Touch the lease to mark the cache as active.
    pub fn touch_lease(&self) -> io::Result<()> {
        Lease::touch(&self.cache_tree)
    }

    /// Remove the lease file.
    pub fn remove_lease(&self) -> io::Result<()> {
        Lease::remove(&self.cache_tree)
    }

    /// Check if the lease exists.
    pub fn lease_exists(&self) -> bool {
        Lease::exists(&self.cache_tree)
    }

    /// Get the age of the lease.
    pub fn lease_age(&self) -> std::time::Duration {
        Lease::age(&self.cache_tree)
    }

    /// Check if the cache is occupied (lease is active).
    pub fn is_occupied(&self) -> bool {
        LeaseAwareStorageProvider::occupied(&self.cache_tree)
    }

    /// Evict the cache if the lease is stale.
    pub fn maybe_evict(&self) -> io::Result<()> {
        LeaseAwareStorageProvider::maybe_evict(&self.cache_tree)
    }
}

impl StorageProvider for HlsStorageProvider {
    type Reader = <FileStorageProvider as StorageProvider>::Reader;
    type Writer = <FileStorageProvider as StorageProvider>::Writer;

    fn into_reader_writer(
        self,
        _content_length: Option<u64>,
    ) -> io::Result<(Self::Reader, Self::Writer)> {
        // HlsStorageProvider is only for caching, not for direct reading/writing
        // The actual reading/writing is handled by a separate segmented provider
        unimplemented!(
            "into_reader_writer not implemented for HlsStorageProvider - use get/put methods for caching"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, Write};
    use stream_download_storage_ext::BlobCache;
    use tempfile::tempdir;

    #[test]
    fn test_hls_cache_factory() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let factory = HlsCacheFactory::new(temp_dir.path());

        let key = HlsCacheKey::new("master_hash/variant_id/filename".to_string());

        // Create cache provider
        let cache = factory.create_cache_provider(&key)?;

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
    fn test_hls_storage_provider() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let master_hash = "test_master_hash";

        let provider = HlsStorageProvider::new(temp_dir.path(), master_hash)?;

        // Test with a key
        let key = HlsCacheKey::new(format!("{}/variant_id/filename", master_hash));

        // Initially should not exist
        assert!(!provider.exists(&key)?);

        // Put data
        let test_data = bytes::Bytes::from_static(b"test data");
        provider.put(&key, test_data.clone())?;

        // Should exist now
        assert!(provider.exists(&key)?);

        // Get data back
        let read_data = provider.get(&key)?.expect("Should have data");
        assert_eq!(read_data, test_data);

        // Test lease operations
        assert!(!provider.lease_exists());

        provider.touch_lease()?;
        assert!(provider.lease_exists());

        let age = provider.lease_age();
        assert!(age < std::time::Duration::from_secs(1));

        assert!(provider.is_occupied());

        provider.remove_lease()?;
        assert!(!provider.lease_exists());

        Ok(())
    }

    #[test]
    fn test_hls_storage_provider_storage_trait() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let provider = HlsStorageProvider::new(temp_dir.path(), "test")?;

        // Verify StorageProvider trait is implemented
        let (reader, writer) = provider.into_reader_writer(None)?;

        // Just check that we got reader/writer objects
        // Note: FileStorageProvider returns BufReader/BufWriter which implement Seek
        let mut reader = reader;
        let mut writer = writer;
        assert!(reader.seek(std::io::SeekFrom::Start(0)).is_ok());
        assert!(writer.seek(std::io::SeekFrom::Start(0)).is_ok());

        Ok(())
    }
}
