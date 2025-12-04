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
//!
//! This file (`lib.rs`) acts as a facade: it re-exports the main
//! types and functions from the internal modules to form the public API
//! of the `stream-download-hls` crate.

mod abr;
mod downloader;
mod manager;
mod model;
mod parser;
mod traits;

pub use crate::model::{
    HlsConfig, HlsError, HlsResult, MasterPlaylist, MediaPlaylist, MediaSegment, NewSegment,
    VariantStream,
};

pub use crate::abr::{AbrConfig, AbrController, PlaybackMetrics};
pub use crate::downloader::{DownloaderConfig, ResourceDownloader};
pub use crate::manager::HlsManager;
pub use crate::model::diff_playlists;
pub use crate::parser::{parse_master_playlist, parse_media_playlist};
pub use crate::traits::{MediaStream, SegmentData};

// Temporary re-export of Duration while it's actively used in public types.
// This may be removed later if the types are better encapsulated.
pub use std::time::Duration;
