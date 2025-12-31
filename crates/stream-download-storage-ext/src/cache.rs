use std::any::Any;
use std::io;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::blob::BlobCache;
use super::kv::{KVStore, TreeStructuredKVStore};
use super::lease::{Lease, LeaseAwareStorageProvider, LeaseFile};
use super::tree::TreeStorageFactory;
use stream_download::storage::StorageProvider;

struct CacheKVTreeShared<P>
where
    P: StorageProvider + Send + Sync,
{
    inner: Arc<Mutex<P>>,
    storage_root: PathBuf,
}

struct CacheKVTree<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    tree: TreeStructuredKVStore<P, K>,
    inner: CacheKVTreeShared<P>,
    _phantom: PhantomData<(P, K)>,
}

impl<P, K> CacheKVTree<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    fn new(factory: TreeStorageFactory<P, K>, inner: Arc<Mutex<P>>, storage_root: PathBuf) -> Self {
        let tree = TreeStructuredKVStore::new(factory);
        let inner_shared = CacheKVTreeShared {
            inner,
            storage_root,
        };

        Self {
            tree,
            inner: inner_shared,
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
        // TODO: Implement this properly
        // For now, we need to access the inner provider
        let _inner = self.inner.inner.lock().map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to acquire lock: {}", e),
            )
        })?;

        // Clone the provider since we can't move it out of the Mutex
        // This requires P to be Clone
        unimplemented!("into_reader_writer not implemented for CacheKVTree")
    }
}

/// Public wrapper for CacheKVTree that implements LeaseAwareStorageProvider
pub struct LeaseAwareCacheTree<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    inner: CacheKVTree<P, K>,
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
        inner: Arc<Mutex<P>>,
        storage_root: PathBuf,
        lease_path: PathBuf,
    ) -> Self {
        let cache_tree = CacheKVTree::new(factory, inner, storage_root.clone());
        let lease = LeaseFile::new(lease_path);

        Self {
            inner: cache_tree,
            lease,
        }
    }

    /// Get the cached blob for a key, if present
    pub fn get(&self, key: &K) -> io::Result<Option<bytes::Bytes>> {
        self.inner.get(key)
    }

    /// Store a blob in the cache for a key, overwriting any existing data
    pub fn put(&self, key: &K, data: bytes::Bytes) -> io::Result<()> {
        self.inner.put(key, data)
    }

    /// Get the length of the cached blob for a key, if present
    pub fn len(&self, key: &K) -> io::Result<Option<u64>> {
        self.inner.len(key)
    }

    /// Check if the cache contains a blob for a key
    pub fn exists(&self, key: &K) -> io::Result<bool> {
        self.inner.exists(key)
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

    fn age(&self) -> Duration {
        self.lease.age()
    }
}

// Explicitly implement LeaseAwareStorageProvider to get default implementations
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
        content_length: Option<u64>,
    ) -> io::Result<(Self::Reader, Self::Writer)> {
        self.inner.into_reader_writer(content_length)
    }
}
