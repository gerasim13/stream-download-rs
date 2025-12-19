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
//! - `model`: Core data structures (playlists, segments, errors).
//! - `parser`: Parsing of master/media M3U8 playlists.
//! - `manager`: `HlsManager` and the logic for handling a single HLS stream.
//! - `abr`: A basic adaptive bitrate (ABR) controller.
//! - `downloader`: A thin wrapper around `stream-download` for fetching resources.
//! - `traits`: High-level traits for abstracting media stream sources.
//! - `stream`: HLS implementation of the `SourceStream` trait.
//! - `storage`: Segmented storage provider for splitting HLS segments into separate files.
//!
//! This file (`lib.rs`) acts as a facade: it re-exports the main
//! types and functions from the internal modules to form the public API
//! of the `stream-download-hls` crate.

mod abr;
mod downloader;
mod manager;
mod model;
mod parser;
mod settings;
mod storage;
mod stream;
mod traits;
mod worker;

pub use crate::abr::{AbrConfig, AbrController, PlaybackMetrics};
pub use crate::downloader::ResourceDownloader;
pub use crate::manager::HlsManager;
pub use crate::model::{
    HlsByteStream, HlsError, HlsErrorKind, HlsResult, HlsStreamError, MasterPlaylist,
    MediaPlaylist, MediaSegment, StreamEvent, VariantId, VariantStream,
};
pub use crate::parser::{parse_master_playlist, parse_media_playlist};
pub use crate::settings::HlsSettings;
pub use crate::storage::SegmentedStorageProvider;
pub use crate::stream::{HlsStream, HlsStreamParams};
pub use crate::traits::{
    MediaStream, NextSegmentResult, SegmentData, SegmentType, StreamMiddleware, apply_middlewares,
};
pub use crate::worker::HlsStreamWorker;

#[cfg(feature = "aes-decrypt")]
mod crypto;
#[cfg(feature = "aes-decrypt")]
pub use crate::crypto::Aes128CbcMiddleware;

pub use bytes::Bytes;
pub use std::time::Duration;
