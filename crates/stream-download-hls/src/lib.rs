//! HLS extension for `stream-download`.
//!
//! This crate is an early PoC intended to explore how HTTP HLS playback
//! can be layered on top of the `stream-download` crate without changing
//! its core abstractions too much.
//!
//! Design goals (for the PoC):
//! - Treat each HLS resource (master playlist, media playlist, segment)
//!   as a separate `stream-download` resource.
//! - Keep this crate focused on HLS-specific concerns (parsing playlists,
//!   tracking segments, basic live updates) while delegating all HTTP and
//!   caching to `stream-download`.
//! - Start simple: single rendition, basic live support, no DRM/keys.
//!
//! This crate is composed of several modules:
//! - `parser`: Parsing of master/media M3U8 playlists and associated types.
//! - `manager`: `HlsManager` and the logic for handling a single HLS stream.
//! - `abr`: A basic adaptive bitrate (ABR) controller.
//! - `downloader`: Network downloader plus cached wrapper for playlists/keys.
//! - `stream`: HLS implementation of the `SourceStream` trait.
//! - `storage`: Segmented storage provider for splitting HLS segments into separate files.
//! - `error`: Unified error types.
//! - `cache::keys`: Deterministic cache/layout helpers (including master hash).
//!
//! This file (`lib.rs`) acts as a facade: it re-exports the main
//! types and functions from the internal modules to form the public API
//! of the `stream-download-hls` crate.

mod abr;
mod cache;
mod downloader;
mod error;
mod manager;

mod parser;
mod settings;
mod storage;
mod stream;
mod worker;

pub use crate::abr::{AbrConfig, AbrController, PlaybackMetrics};
pub use crate::downloader::HlsByteStream;
pub use crate::downloader::{CachedBytes, CachedResourceDownloader, ResourceDownloader};
pub use crate::error::{HlsError, HlsResult};

// Deterministic cache/layout helpers
pub use crate::cache::keys::master_hash_from_url;
pub use crate::manager::{
    HlsManager, NextSegmentDescResult, NextSegmentResult, SegmentData, SegmentDescriptor,
    SegmentType,
};
pub use crate::parser::{
    CodecInfo, ContainerFormat, EncryptionMethod, InitSegment, KeyInfo, MasterPlaylist,
    MediaPlaylist, MediaSegment, SegmentKey, VariantId, VariantStream,
};
pub use crate::settings::HlsSettings;
pub use crate::storage::SegmentedStorageProvider;
pub use crate::stream::StreamEvent;

// File-tree (persistent) segment storage helpers (deterministic naming/layout).
pub use crate::storage::hls_factory::HlsFileTreeSegmentFactory;

// Cache/policy layer (leases + eviction) wrapping a segment factory.
pub use crate::storage::cache_layer::HlsCacheLayer;

/// Default persistent HLS segmented storage provider type.
///
/// This is the recommended provider for "real disk caching":
/// - segments: `<storage_root>/<master_hash>/<variant_id>/<segment_basename>`
/// - small resources (`StoreResource`): `<storage_root>/<resource_key_as_path>`
/// - LRU eviction and leases are enabled via the underlying cache layer.
///
/// Use [`HlsPersistentStorageProvider::new_hls_file_tree`] to construct it without specifying
/// generics.
pub type HlsPersistentStorageProvider =
    SegmentedStorageProvider<HlsCacheLayer<HlsFileTreeSegmentFactory>>;

pub use crate::manager::{MediaStream, StreamMiddleware};
pub use crate::stream::{HlsStream, HlsStreamParams};
pub use crate::worker::HlsStreamWorker;

#[cfg(feature = "aes-decrypt")]
mod crypto;
#[cfg(feature = "aes-decrypt")]
pub use crate::crypto::{Aes128CbcMiddleware, KeyProcessorCallback};

pub use bytes::Bytes;
pub use std::time::Duration;
