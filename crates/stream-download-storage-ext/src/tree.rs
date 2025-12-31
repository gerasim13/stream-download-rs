use std::any::Any;
use std::io;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use educe::Educe;

use super::blob::StorageBackedBlobCache;
use stream_download::storage::StorageProvider;

/// Type alias for factory functions that create cache nodes from keys.
pub type CacheFactory<P, K> =
    dyn Fn(&K) -> io::Result<Option<StorageBackedBlobCache<P>>> + Sync + Send;

/// Trait for a storage tree.
pub trait Tree {
    type Provider: StorageProvider;
    type Key: Any;
    type Factory: Fn(&Self::Key) -> io::Result<Option<StorageBackedBlobCache<Self::Provider>>>
        + Sync
        + Send
        + ?Sized;

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
pub struct TreeStorageFactory<P, K>
where
    P: StorageProvider + Sync,
    K: Any,
{
    #[educe(Debug = false)]
    factory: Arc<Box<CacheFactory<P, K>>>,
    storage_root: PathBuf,
    _phantom: PhantomData<(P, K)>,
}

impl<P, K> TreeStorageFactory<P, K>
where
    P: StorageProvider + Sync,
    K: Any,
{
    pub fn new(
        factory: impl Fn(&K) -> io::Result<Option<StorageBackedBlobCache<P>>> + Sync + Send + 'static,
        storage_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            factory: Arc::new(Box::new(factory)),
            storage_root: storage_root.into(),
            _phantom: PhantomData,
        }
    }
}

impl<P, K> Tree for TreeStorageFactory<P, K>
where
    P: StorageProvider + Sync,
    K: Any,
{
    type Provider = P;
    type Key = K;
    type Factory = CacheFactory<P, K>;

    fn factory(&self) -> &Arc<Box<Self::Factory>> {
        &self.factory
    }
}
