use std::any::Any;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use reqwest::Url;

use super::blob::BlobCache;
use super::file::FileStorageProvider;
use super::kv::TreeStructuredKVStore;
use super::lease::LeaseAwareStorageProvider;
use super::tree::{Tree, TreeStorageFactory};
use stream_download::storage::{StorageProvider, StorageReader, StorageWriter};

pub struct CacheKVTreeShared<P>
where
    P: StorageProvider,
{
    inner: Arc<Mutex<P>>,
    storage_root: PathBuf,
}

pub struct CacheKVTree<P, K>
where
    P: StorageProvider,
    K: Any + Send,
{
    tree: TreeStructuredKVStore<P, K>,
    inner: CacheKVTreeShared<P>,
    _phantom: PhantomData<(P, K)>,
}

impl<P, K> StorageProvider for CacheKVTree<P, K>
where
    P: StorageProvider,
    K: Any + Send,
{
    type Reader = P::Reader;
    type Writer = P::Writer;

    fn into_reader_writer(
        self,
        _content_length: Option<u64>,
    ) -> io::Result<(Self::Reader, Self::Writer)> {
        unimplemented!()
    }
}
