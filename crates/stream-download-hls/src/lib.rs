//! HLS support for `stream-download`.
//!
//! This crate provides an HLS implementation and re-exports its public API from internal modules.
//! For design notes and higher-level documentation, see `crates/stream-download-hls/README.md`.

// mod abr;
pub mod cache;
mod downloader;
mod error;
// mod manager;

pub mod parser;
pub mod segmented_provider;
mod settings;
// mod storage;
pub mod storage_new;
mod stream;
// mod worker;

// pub use crate::abr::{AbrConfig, AbrController, AbrDecision};

pub use crate::downloader::HlsByteStream;
pub use crate::downloader::{
    CacheDownloader, CachedBytes, Downloader, DownloaderBuilder, DownloaderExt, HttpDownloader,
    Resource, RetryDownloader, RetryPolicy, TimeoutDownloader, create_default_downloader,
};
pub use crate::error::{HlsError, HlsResult};

/// Deterministic cache/layout helper.
pub use crate::cache::factory::{
    create_hls_cache_tree, create_hls_factory, create_hls_tree_factory,
};
pub use crate::cache::keys::{
    CacheKeyGenerator, HlsCacheKey, create_key_callback, master_hash_from_url,
};
// pub use crate::manager::{HlsManager, NextSegmentResult, SegmentDescriptor};
pub use crate::parser::{
    CodecInfo, ContainerFormat, EncryptionMethod, InitSegment, KeyInfo, MasterPlaylist,
    MediaPlaylist, MediaSegment, SegmentKey, VariantId, VariantStream,
};
pub use crate::segmented_provider::SegmentedStorageProvider;
pub use crate::settings::HlsSettings;
// pub use crate::storage::SegmentedStorageProvider as LegacySegmentedStorageProvider;
pub use crate::storage_new::HlsStorageProvider;
pub use crate::stream::{HlsCommand, StreamEvent};

/// File-tree (persistent) segment storage helper (deterministic naming/layout).
// pub use crate::storage::hls_factory::HlsFileTreeSegmentFactory;

/// Cache/policy layer (leases + eviction) wrapping a segment factory.
// pub use crate::storage::cache_layer::HlsCacheLayer;

/// Default persistent segmented storage provider for disk-backed caching.
///
/// Use [`SegmentedStorageProvider::new_hls_file_tree`] to construct it.
// pub type HlsPersistentStorageProvider =
//     SegmentedStorageProvider<HlsCacheLayer<HlsFileTreeSegmentFactory>>;

// pub use crate::manager::{MediaStream, StreamMiddleware};
pub use crate::stream::{HlsStream, HlsStreamParams};
// pub use crate::worker::HlsStreamWorker;

// Temporarily disabled for new architecture
// #[cfg(feature = "aes-decrypt")]
// mod crypto;
// #[cfg(feature = "aes-decrypt")]
// pub use crate::crypto::middleware::Aes128CbcMiddleware;
// #[cfg(feature = "aes-decrypt")]
// pub use crate::crypto::resolver::AesKeyResolver;
// #[cfg(feature = "aes-decrypt")]
// pub use crate::crypto::resolver::KeyProcessorCallback;
pub use bytes::Bytes;
pub use std::time::Duration;
