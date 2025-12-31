use std::any::Any;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use educe::Educe;
use reqwest::Url;

use super::blob::{BlobCache, StorageBackedBlobCache};
use stream_download::storage::StorageProvider;

/// Trait for a storage tree.
pub trait Tree {
    type Provider: StorageProvider;
    type Key: Any;
    type Factory: Fn(&Self::Key) -> io::Result<Option<StorageBackedBlobCache<Self::Provider>>>
        + Sync
        + Send;

    /// Returns a factory function that creates a new node in the storage tree.
    fn factory(&self) -> &Arc<Box<Self::Factory>>;

    /// Returns a node in the storage tree.
    fn node(&self, key: &Self::Key) -> io::Result<Option<StorageBackedBlobCache<Self::Provider>>> {
        let factory = self.factory();
        factory(key)
    }
}

/// A cache tree backed by a filesystem.
#[derive(Educe)]
#[educe(Debug)]
pub struct TreeStorageFactory<P, K, F>
where
    P: StorageProvider,
    K: Any,
    F: Fn(&K) -> io::Result<Option<StorageBackedBlobCache<P>>> + Sync + Send,
{
    #[educe(Debug = false)]
    factory: Arc<Box<F>>,
    storage_root: PathBuf,
    _phantom: PhantomData<(P, K)>,
}

impl<P, K, F> TreeStorageFactory<P, K, F>
where
    P: StorageProvider,
    K: Any,
    F: Fn(&K) -> io::Result<Option<StorageBackedBlobCache<P>>> + Sync + Send,
{
    pub fn new(factory: F, storage_root: impl Into<PathBuf>) -> Self {
        Self {
            factory: Arc::new(Box::new(factory)),
            storage_root: storage_root.into(),
            _phantom: PhantomData,
        }
    }
}

impl<P, K, F> Tree for TreeStorageFactory<P, K, F>
where
    P: StorageProvider,
    K: Any,
    F: Fn(&K) -> io::Result<Option<StorageBackedBlobCache<P>>> + Sync + Send,
{
    type Provider = P;
    type Key = K;
    type Factory = F;

    fn factory(&self) -> &Arc<Box<Self::Factory>> {
        &self.factory
    }
}
