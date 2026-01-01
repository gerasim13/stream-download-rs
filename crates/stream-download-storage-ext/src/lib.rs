//! Storage-backed cache implementations for stream-download.
//!
//! This crate provides cache implementations that work exclusively through
//! the `StorageProvider` interface from `stream-download`, without any direct
//! filesystem access. All cached data is stored as `bytes::Bytes` for efficient
//! memory management and zero-copy operations where possible.
mod blob;
mod cache;
mod file;
mod kv;
mod lease;
mod tree;

pub use blob::{BlobCache, BlobCacheReader, BlobCacheWriter, StorageBackedBlobCache};
pub use cache::LeaseAwareCacheTree;
pub use file::FileStorageProvider;
pub use kv::{KVStore, TreeStructuredKVStore};
pub use lease::{Lease, LeaseAwareStorageProvider};
pub use tree::{Tree, TreeStorageFactory};
