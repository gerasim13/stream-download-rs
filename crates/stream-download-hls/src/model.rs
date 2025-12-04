/stream-download-rs/crates/stream-download-hls/src/model.rs#L1-260
//! Core data models and error types used by the `stream-download-hls` crate.
//!
//! This module is intentionally focused on *pure* types, with no networking
//! or I/O concerns. Higher-level modules (`downloader`, `manager`, `abr`)
//! build on top of these types.
//!
//! Scope for the PoC:
//! - Minimal but useful representation of master and media playlists.
//! - Simple error and result types that other modules can reuse.
//! - A small helper (`diff_playlists`) for detecting new segments in live
//!   playlists.

use std::time::Duration;

/// Result type used by this crate.
pub type HlsResult<T> = Result<T, HlsError>;

/// Error type for the HLS extension crate.
///
/// The goal for the PoC is to keep this reasonably small and ergonomic,
/// while leaving room to extend it later (e.g. mapping to `stream_download`
/// error types more explicitly).
#[derive(Debug, thiserror::Error)]
pub enum HlsError {
    /// A generic error with a message.
    #[error("{0}")]
    Message(String),

    /// Errors related to invalid or unsupported playlist contents.
    #[error("invalid playlist: {0}")]
    InvalidPlaylist(String),

    /// An error wrapping lower-level I/O or HTTP issues.
    ///
    /// For now this is just a string; later it can wrap concrete error
    /// types from `stream-download` or `reqwest` if needed.
    #[error("I/O error: {0}")]
    Io(String),
}

impl HlsError {
    /// Convenience helper to construct a simple message error.
    pub fn msg(msg: impl Into<String>) -> Self {
        HlsError::Message(msg.into())
    }
}

impl From<std::io::Error> for HlsError {
    fn from(err: std::io::Error) -> Self {
        HlsError::Io(err.to_string())
    }
}

/// Basic representation of a master playlist.
///
/// This intentionally captures only a small subset of HLS metadata needed
/// to bootstrap playback and choose a single variant.
#[derive(Debug, Clone)]
pub struct MasterPlaylist {
    /// List of available variants (renditions).
    pub variants: Vec<VariantStream>,
}

/// One `#EXT-X-STREAM-INF` entry in the master playlist.
#[derive(Debug, Clone)]
pub struct VariantStream {
    /// Absolute or relative URL of the media playlist for this variant.
    pub uri: String,
    /// Optional advertised bandwidth in bits per second.
    pub bandwidth: Option<u64>,
    /// Optional human-readable name (e.g., "720p", "audio-en").
    pub name: Option<String>,
}

/// Basic representation of a media playlist (VOD or live).
///
/// This is intentionally simplified for the PoC:
/// - `EXT-X-DISCONTINUITY` and other advanced tags are ignored for now.
/// - `EXT-X-KEY` / encryption are not modeled yet.
/// - Only enough information to sequence segments and detect new ones.
#[derive(Debug, Clone)]
pub struct MediaPlaylist {
    /// List of segments in the order they appear.
    pub segments: Vec<MediaSegment>,
    /// Target segment duration if present.
    pub target_duration: Option<Duration>,
    /// Media sequence number of the first segment.
    pub media_sequence: u64,
    /// Whether the playlist is finished (VOD or live that ended).
    pub end_list: bool,
}

/// One `#EXTINF` (media segment) entry.
#[derive(Debug, Clone)]
pub struct MediaSegment {
    /// Sequence number of the segment (media-sequence + index in playlist).
    pub sequence: u64,
    /// URL of the segment (absolute or relative to playlist URI).
    pub uri: String,
    /// Duration of the segment if known.
    pub duration: Duration,
}

/// Represents a newly discovered segment when comparing two media playlists.
#[derive(Debug, Clone)]
pub struct NewSegment {
    /// The new segment.
    pub segment: MediaSegment,
}

/// Simple configuration for the PoC HLS manager.
///
/// This will likely grow over time (buffer sizes, ABR knobs, etc.).
#[derive(Debug, Clone)]
pub struct HlsConfig {
    /// Optional override for how often live playlists should be refreshed.
    /// If not set, `target_duration` from the playlist should be used.
    pub live_refresh_interval: Option<Duration>,
}

impl Default for HlsConfig {
    fn default() -> Self {
        Self {
            live_refresh_interval: None,
        }
    }
}

/// Diff two media playlists and return newly appeared segments.
///
/// The simplest working strategy for HLS live is:
/// - Treat `media_sequence` as the sequence number for the first entry.
/// - Any segment with a sequence greater than the max sequence in the
///   old playlist is considered new.
///
/// This does not handle re-ordering or discontinuities, which is fine
/// for an initial PoC.
///
/// This function is pure and independent from I/O and can be tested
/// separately.
pub fn diff_playlists(old: &MediaPlaylist, new: &MediaPlaylist) -> Vec<NewSegment> {
    let max_old_seq = old
        .segments
        .iter()
        .map(|s| s.sequence)
        .max()
        .unwrap_or_else(|| old.media_sequence.saturating_sub(1));

    new.segments
        .iter()
        .filter(|seg| seg.sequence > max_old_seq)
        .cloned()
        .map(|segment| NewSegment { segment })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_playlists_no_old_segments_returns_all_new() {
        let old = MediaPlaylist {
            segments: Vec::new(),
            target_duration: None,
            media_sequence: 1,
            end_list: false,
        };

        let new = MediaPlaylist {
            segments: vec![
                MediaSegment {
                    sequence: 1,
                    uri: "seg1.ts".into(),
                    duration: Duration::from_secs(4),
                },
                MediaSegment {
                    sequence: 2,
                    uri: "seg2.ts".into(),
                    duration: Duration::from_secs(4),
                },
            ],
            target_duration: None,
            media_sequence: 1,
            end_list: false,
        };

        let diff = diff_playlists(&old, &new);
        assert_eq!(diff.len(), 2);
        assert_eq!(diff[0].segment.sequence, 1);
        assert_eq!(diff[1].segment.sequence, 2);
    }

    #[test]
    fn diff_playlists_filters_only_truly_new_segments() {
        let old = MediaPlaylist {
            segments: vec![
                MediaSegment {
                    sequence: 5,
                    uri: "seg5.ts".into(),
                    duration: Duration::from_secs(4),
                },
                MediaSegment {
                    sequence: 6,
                    uri: "seg6.ts".into(),
                    duration: Duration::from_secs(4),
                },
            ],
            target_duration: None,
            media_sequence: 5,
            end_list: false,
        };

        let new = MediaPlaylist {
            segments: vec![
                MediaSegment {
                    sequence: 6,
                    uri: "seg6.ts".into(),
                    duration: Duration::from_secs(4),
                },
                MediaSegment {
                    sequence: 7,
                    uri: "seg7.ts".into(),
                    duration: Duration::from_secs(4),
                },
                MediaSegment {
                    sequence: 8,
                    uri: "seg8.ts".into(),
                    duration: Duration::from_secs(4),
                },
            ],
            target_duration: None,
            media_sequence: 6,
            end_list: false,
        };

        let diff = diff_playlists(&old, &new);
        assert_eq!(diff.len(), 2);
        assert_eq!(diff[0].segment.sequence, 7);
        assert_eq!(diff[1].segment.sequence, 8);
    }
}
