use std::any::Any;
use std::io;
use std::marker::PhantomData;
use std::path::PathBuf;

use super::kv::{KVStore, TreeStructuredKVStore};
use super::lease::{Lease, LeaseAwareStorageProvider, LeaseFile};
use super::tree::TreeStorageFactory;
use stream_download::storage::StorageProvider;

#[derive(Debug)]
struct CacheKVTree<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    tree: TreeStructuredKVStore<P, K>,
    _phantom: PhantomData<(P, K)>,
}

impl<P, K> CacheKVTree<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    fn new(factory: TreeStorageFactory<P, K>, _storage_root: PathBuf) -> Self {
        let tree = TreeStructuredKVStore::new(factory);

        Self {
            tree,
            _phantom: PhantomData,
        }
    }

    fn get(&self, key: &K) -> io::Result<Option<bytes::Bytes>> {
        KVStore::get(&self.tree, key)
    }

    fn put(&self, key: &K, data: bytes::Bytes) -> io::Result<()> {
        KVStore::put(&self.tree, key, data)
    }

    fn len(&self, key: &K) -> io::Result<Option<u64>> {
        KVStore::len(&self.tree, key)
    }

    fn exists(&self, key: &K) -> io::Result<bool> {
        KVStore::exists(&self.tree, key)
    }
}

impl<P, K> StorageProvider for CacheKVTree<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    type Reader = P::Reader;
    type Writer = P::Writer;

    fn into_reader_writer(
        self,
        _content_length: Option<u64>,
    ) -> io::Result<(Self::Reader, Self::Writer)> {
        // CacheKVTree is only for caching, not for direct reading/writing
        unimplemented!(
            "into_reader_writer not implemented for CacheKVTree - use get/put methods instead"
        )
    }
}

/// Public wrapper for CacheKVTree that implements LeaseAwareStorageProvider
#[derive(Debug)]
pub struct LeaseAwareCacheTree<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    cache: CacheKVTree<P, K>,
    lease: LeaseFile,
}

impl<P, K> LeaseAwareCacheTree<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    /// Create a new lease-aware cache tree
    pub fn new(
        factory: TreeStorageFactory<P, K>,
        storage_root: PathBuf,
        lease_path: PathBuf,
    ) -> Self {
        let cache = CacheKVTree::new(factory, storage_root);
        let lease = LeaseFile::new(lease_path);

        Self { cache, lease }
    }

    /// Get the cached blob for a key, if present
    pub fn get(&self, key: &K) -> io::Result<Option<bytes::Bytes>> {
        self.cache.get(key)
    }

    /// Store a blob in the cache for a key, overwriting any existing data
    pub fn put(&self, key: &K, data: bytes::Bytes) -> io::Result<()> {
        self.cache.put(key, data)
    }

    /// Get the length of the cached blob for a key, if present
    pub fn len(&self, key: &K) -> io::Result<Option<u64>> {
        self.cache.len(key)
    }

    /// Check if the cache contains a blob for a key
    pub fn exists(&self, key: &K) -> io::Result<bool> {
        self.cache.exists(key)
    }
}

impl<P, K> Lease for LeaseAwareCacheTree<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    fn touch(&self) -> io::Result<()> {
        self.lease.touch()
    }

    fn remove(&self) -> io::Result<()> {
        self.lease.remove()
    }

    fn exists(&self) -> bool {
        self.lease.exists()
    }

    fn age(&self) -> std::time::Duration {
        self.lease.age()
    }
}

impl<P, K> LeaseAwareStorageProvider for LeaseAwareCacheTree<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
}

impl<P, K> StorageProvider for LeaseAwareCacheTree<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    type Reader = P::Reader;
    type Writer = P::Writer;

    fn into_reader_writer(
        self,
        _content_length: Option<u64>,
    ) -> io::Result<(Self::Reader, Self::Writer)> {
        // LeaseAwareCacheTree is only for caching, not for direct reading/writing
        unimplemented!(
            "into_reader_writer not implemented for LeaseAwareCacheTree - use get/put methods instead"
        )
    }
}
