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

use std::fmt::Debug;

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
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

/// Uniquely identifies a variant within a master playlist.
///
/// In most cases, the index in the `variants` vector is sufficient, but a dedicated
/// type makes the API clearer and more type-safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VariantId(pub usize);

/// Supported container formats for a variant or segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerFormat {
    /// MPEG-2 Transport Stream.
    Ts,
    /// Fragmented MP4.
    Fmp4,
    /// Any other format we don't explicitly handle yet.
    Other,
}

/// Basic codec and format information extracted from playlist attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecInfo {
    /// The raw `CODECS="..."` string from the playlist.
    /// Can be used by the player to get detailed information.
    pub codecs: Option<String>,
    /// A best-effort guess at the audio codec.
    pub audio_codec: Option<String>,
    /// A best-effort guess at the container format.
    pub container: Option<ContainerFormat>,
}

/// Supported HLS encryption methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncryptionMethod {
    /// No encryption (`METHOD=NONE`).
    None,
    /// AES-128 CBC encryption of the whole segment.
    Aes128,
    /// Sample-based AES encryption.
    SampleAes,
    /// Any other method, stored as a raw string for forward compatibility.
    Other(String),
}

/// Represents a single `#EXT-X-KEY` entry from a media playlist.
#[derive(Debug, Clone)]
pub struct KeyInfo {
    /// The encryption method to be used.
    pub method: EncryptionMethod,
    /// The URI of the encryption key. Can be relative to the playlist.
    pub uri: Option<String>,
    /// The initialization vector (IV), if specified.
    pub iv: Option<[u8; 16]>,
    /// The key format, e.g., "identity".
    pub key_format: Option<String>,
    /// The key format version(s).
    pub key_format_versions: Option<String>,
}

/// The effective encryption key applicable to a specific segment.
#[derive(Debug, Clone)]
pub struct SegmentKey {
    /// The encryption method that applies to this segment.
    pub method: EncryptionMethod,
    /// A reference to the full key information. This allows the player to
    /// fetch the key from the URI if needed.
    pub key_info: Option<KeyInfo>,
}

/// Callback type used to transform raw key bytes fetched from a key server
/// before they are used for decryption. This allows custom key wrapping/DRM flows.
pub type KeyProcessorCallback = dyn Fn(Bytes) -> Bytes + Send + Sync;

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
    /// A unique identifier for this variant within the context of its master playlist.
    pub id: VariantId,
    /// Absolute or relative URL of the media playlist for this variant.
    pub uri: String,
    /// Optional advertised bandwidth in bits per second.
    pub bandwidth: Option<u64>,
    /// Optional human-readable name (e.g., "720p", "audio-en").
    pub name: Option<String>,
    /// Codec and format information for this variant.
    pub codec: Option<CodecInfo>,
}

/// Basic representation of a media playlist (VOD or live).
///
/// This is intentionally simplified for the PoC:
/// - `EXT-X-DISCONTINUITY` and other advanced tags are ignored for now.
/// - Only enough information to sequence segments and detect new ones.
#[derive(Debug, Clone)]
pub struct InitSegment {
    /// URL of the initialization segment (absolute or relative to playlist URI).
    pub uri: String,
    /// Optional encryption information effective for this init segment.
    pub key: Option<SegmentKey>,
}

#[derive(Debug, Clone)]
pub struct MediaPlaylist {
    /// List of segments in the order they appear.
    pub segments: Vec<MediaSegment>,
    /// Target segment duration if present.
    pub target_duration: Option<Duration>,
    /// Optional initialization segment (for fMP4 streams).
    pub init_segment: Option<InitSegment>,
    /// Media sequence number of the first segment.
    pub media_sequence: u64,
    /// Whether the playlist is finished (VOD or live that ended).
    pub end_list: bool,
    /// The last seen `#EXT-X-KEY` for this playlist. This applies to all
    /// subsequent segments until a new key tag is encountered.
    pub current_key: Option<KeyInfo>,
}

/// One `#EXTINF` (media segment) entry.
#[derive(Debug, Clone)]
pub struct MediaSegment {
    /// Sequence number of the segment (media-sequence + index in playlist).
    pub sequence: u64,
    /// The variant this segment belongs to. This is crucial for the player
    /// to manage decoders.
    pub variant_id: VariantId,
    /// URL of the segment (absolute or relative to playlist URI).
    pub uri: String,
    /// Duration of the segment if known.
    pub duration: Duration,
    /// Optional encryption information effective for this segment.
    pub key: Option<SegmentKey>,
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
#[derive(Clone)]
pub struct HlsConfig {
    /// Optional override for how often live playlists should be refreshed.
    /// If not set, `target_duration` from the playlist should be used.
    pub live_refresh_interval: Option<Duration>,

    /// Optional callback to post-process a fetched AES key before use (e.g., unwrap DRM).
    ///
    /// Not Debug to keep HlsConfig debuggable without requiring function pointers to implement Debug.
    pub key_processor_cb: Option<Arc<Box<KeyProcessorCallback>>>,
    /// Optional query parameters appended to key fetch requests.
    pub key_query_params: Option<HashMap<String, String>>,
    /// Optional headers added to key fetch requests.
    pub key_request_headers: Option<HashMap<String, String>>,
}
impl Debug for HlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HlsConfig")
            .field("live_refresh_interval", &self.live_refresh_interval)
            .field("key_query_params", &self.key_query_params)
            .field("key_request_headers", &self.key_request_headers)
            .finish()
    }
}

impl Default for HlsConfig {
    fn default() -> Self {
        Self {
            live_refresh_interval: None,
            key_processor_cb: None,
            key_query_params: None,
            key_request_headers: None,
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
            init_segment: None,
            current_key: None,
            media_sequence: 1,
            end_list: false,
        };

        let new = MediaPlaylist {
            segments: vec![
                MediaSegment {
                    sequence: 1,
                    variant_id: VariantId(0),
                    uri: "seg1.ts".into(),
                    duration: Duration::from_secs(4),
                    key: None,
                },
                MediaSegment {
                    sequence: 2,
                    variant_id: VariantId(0),
                    uri: "seg2.ts".into(),
                    duration: Duration::from_secs(4),
                    key: None,
                },
            ],
            target_duration: None,
            init_segment: None,
            current_key: None,
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
                    variant_id: VariantId(0),
                    uri: "seg5.ts".into(),
                    duration: Duration::from_secs(4),
                    key: None,
                },
                MediaSegment {
                    sequence: 6,
                    variant_id: VariantId(0),
                    uri: "seg6.ts".into(),
                    duration: Duration::from_secs(4),
                    key: None,
                },
            ],
            target_duration: None,
            init_segment: None,
            media_sequence: 5,
            end_list: false,
            current_key: None,
        };

        let new = MediaPlaylist {
            segments: vec![
                MediaSegment {
                    sequence: 6,
                    variant_id: VariantId(0),
                    uri: "seg6.ts".into(),
                    duration: Duration::from_secs(4),
                    key: None,
                },
                MediaSegment {
                    sequence: 7,
                    variant_id: VariantId(0),
                    uri: "seg7.ts".into(),
                    duration: Duration::from_secs(4),
                    key: None,
                },
                MediaSegment {
                    sequence: 8,
                    variant_id: VariantId(0),
                    uri: "seg8.ts".into(),
                    duration: Duration::from_secs(4),
                    key: None,
                },
            ],
            target_duration: None,
            init_segment: None,
            media_sequence: 6,
            end_list: false,
            current_key: None,
        };

        let diff = diff_playlists(&old, &new);
        assert_eq!(diff.len(), 2);
        assert_eq!(diff[0].segment.sequence, 7);
        assert_eq!(diff[1].segment.sequence, 8);
    }
}
