/stream-download-rs/crates/stream-download-hls/src/parser.rs#L1-200
//! Playlist parser module (stub).
//!
//! This module will eventually contain the logic for parsing HLS master and
//! media playlists (M3U8). For now it only provides function stubs so that
//! the rest of the crate can be wired up and compiled.
//!
//! Design goals for the real implementation:
//! - No direct dependency on `hls-client` (use it only as a reference).
//! - Support for both VOD and live playlists.
//! - Minimal but robust handling of core tags:
//!   - Master: `#EXTM3U`, `#EXT-X-STREAM-INF`, optional `#EXT-X-MEDIA`.
//!   - Media: `#EXTINF`, `#EXT-X-TARGETDURATION`, `#EXT-X-MEDIA-SEQUENCE`,
//!            `#EXT-X-ENDLIST`.
//! - Focus on correctness and clear error reporting over micro-optimizations.
//!
//! The functions in this file currently return `HlsError::InvalidPlaylist`
//! and are meant to be replaced with a real implementation later.

use crate::model::{HlsError, HlsResult, MasterPlaylist, MediaPlaylist};

/// Parse a master playlist (M3U8) into a [`MasterPlaylist`].
///
/// Expected responsibilities of the future implementation:
/// - Validate the `#EXTM3U` header.
/// - Parse all `#EXT-X-STREAM-INF` entries and their following URIs.
/// - Extract basic attributes such as `BANDWIDTH` and `NAME`.
/// - Tolerate unknown tags (ignore or log them) as per HLS spec recommendations.
pub fn parse_master_playlist(_data: &[u8]) -> HlsResult<MasterPlaylist> {
    Err(HlsError::InvalidPlaylist(
        "parse_master_playlist is not implemented yet".to_string(),
    ))
}

/// Parse a media playlist (M3U8) into a [`MediaPlaylist`].
///
/// Expected responsibilities of the future implementation:
/// - Validate the `#EXTM3U` header.
/// - Parse `#EXT-X-TARGETDURATION`, `#EXT-X-MEDIA-SEQUENCE`, `#EXT-X-ENDLIST`.
/// - For each `#EXTINF` line, parse the duration and following URI into a
///   `MediaSegment`.
/// - Compute segment `sequence` numbers based on `media_sequence` and index.
/// - Tolerate unknown/unsupported tags where possible, to be spec-friendly.
pub fn parse_media_playlist(_data: &[u8]) -> HlsResult<MediaPlaylist> {
    Err(HlsError::InvalidPlaylist(
        "parse_media_playlist is not implemented yet".to_string(),
    ))
}
