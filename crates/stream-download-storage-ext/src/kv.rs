use std::any::Any;
use std::io;
use std::marker::PhantomData;

use super::blob::BlobCache;
use super::tree::{Tree, TreeStorageFactory};
use stream_download::storage::StorageProvider;

/// Trait for a key-value store.
pub trait KVStore {
    type Factory: Tree + Sync + Send;

    /// Factory for creating nodes.
    fn factory(&self) -> &Self::Factory;

    /// Get the cached blob, if present.
    fn get(&self, key: &<Self::Factory as Tree>::Key) -> io::Result<Option<bytes::Bytes>> {
        if let Some(node) = self.factory().node(key)? {
            BlobCache::get(&node)
        } else {
            Ok(None)
        }
    }

    /// Store a blob in the cache, overwriting any existing data.
    fn put(&self, key: &<Self::Factory as Tree>::Key, data: bytes::Bytes) -> io::Result<()> {
        if let Some(node) = self.factory().node(key)? {
            BlobCache::put(&node, data)
        } else {
            Ok(())
        }
    }

    /// Get the length of the cached blob, if present.
    fn len(&self, key: &<Self::Factory as Tree>::Key) -> io::Result<Option<u64>> {
        if let Some(node) = self.factory().node(key)? {
            BlobCache::len(&node)
        } else {
            Ok(None)
        }
    }

    /// Check if the cache contains a blob.
    fn exists(&self, key: &<Self::Factory as Tree>::Key) -> io::Result<bool> {
        if let Some(node) = self.factory().node(key)? {
            BlobCache::exists(&node)
        } else {
            Ok(false)
        }
    }
}

/// Storage-backed key-value store
#[derive(Debug)]
pub struct TreeStructuredKVStore<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    factory: TreeStorageFactory<P, K>,
    _phantom: PhantomData<(P, K)>,
}

impl<P, K> TreeStructuredKVStore<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    pub fn new(factory: TreeStorageFactory<P, K>) -> Self {
        Self {
            factory,
            _phantom: PhantomData,
        }
    }
}

impl<P, K> KVStore for TreeStructuredKVStore<P, K>
where
    P: StorageProvider + Send + Sync,
    K: Any + Send + Sync,
{
    type Factory = TreeStorageFactory<P, K>;

    fn factory(&self) -> &Self::Factory {
        &self.factory
    }
}
