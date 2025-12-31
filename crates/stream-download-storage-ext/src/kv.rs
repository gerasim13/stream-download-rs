use std::any::Any;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use reqwest::Url;

use super::blob::{BlobCache, StorageBackedBlobCache};
use super::file::FileStorageProvider;
use super::lease::LeaseAwareStorageProvider;
use super::tree::{Tree, TreeStorageFactory};
use stream_download::storage::{StorageProvider, StorageReader, StorageWriter};

/// Trait for a key-value store.
pub trait KVStore {
    type Factory: Tree + Sync + Send;

    /// Factory for creating nodes.
    fn factory(&self) -> &Self::Factory;

    /// Get the cached blob, if present.
    fn get(&self, key: &<Self::Factory as Tree>::Key) -> io::Result<Option<Bytes>> {
        if let Some(node) = self.factory().node(key)? {
            node.get()
        } else {
            Ok(None)
        }
    }

    /// Store a blob in the cache, overwriting any existing data.
    fn put(&self, key: &<Self::Factory as Tree>::Key, data: Bytes) -> io::Result<()> {
        if let Some(node) = self.factory().node(key)? {
            node.put(data)
        } else {
            Ok(())
        }
    }

    /// Get the length of the cached blob, if present.
    fn len(&self, key: &<Self::Factory as Tree>::Key) -> io::Result<Option<u64>> {
        if let Some(node) = self.factory().node(key)? {
            node.len()
        } else {
            Ok(None)
        }
    }

    /// Check if the cache contains a blob.
    fn exists(&self, key: &<Self::Factory as Tree>::Key) -> io::Result<bool> {
        if let Some(node) = self.factory().node(key)? {
            node.exists()
        } else {
            Ok(false)
        }
    }
}

/// Storage-backed key-value store
pub struct TreeStructuredKVStore<P, K>
where
    P: StorageProvider,
    K: Any,
{
    factory: TreeStorageFactory<P, K>,
    _phantom: PhantomData<(P, K)>,
}

impl<P, K> KVStore for TreeStructuredKVStore<P, K>
where
    P: StorageProvider,
    K: Any,
{
    type Factory = TreeStorageFactory<P, K>;

    fn factory(&self) -> &Self::Factory {
        &self.factory
    }
}
