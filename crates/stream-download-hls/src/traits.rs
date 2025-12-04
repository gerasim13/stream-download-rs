//! High-level asynchronous traits for consuming media streams.
//!
//! This module defines the `MediaStream` trait, which provides a protocol-agnostic
//! contract for a player to interact with a streaming source like HLS. The goal
//! is to abstract away the complexity of playlist parsing, live updates, and
//! variant switching, offering a simple `next_segment` iterator-like API.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::model::{CodecInfo, HlsResult, SegmentKey, VariantId, VariantStream};

/// A data package for a single media segment.
///
/// This struct bundles the raw segment bytes with the necessary metadata for a
/// player to decode and schedule it correctly. It's the primary type returned
/// by the `MediaStream` trait.
#[derive(Debug, Clone)]
pub struct SegmentData {
    /// Raw bytes of the media segment (e.g., a TS or fMP4 file).
    /// This data may still be encrypted if a `key` is present and the underlying
    /// implementation has not performed decryption.
    pub data: Bytes,
    /// Uniquely identifies the variant (stream/rendition) this segment belongs to.
    /// A player can use this to detect when an adaptive bitrate switch has occurred.
    pub variant_id: VariantId,
    /// Codec and container information for the associated variant.
    /// The player uses this to initialize or re-initialize a decoder.
    pub codec_info: Option<CodecInfo>,
    /// Encryption key information, if the segment is encrypted.
    /// It's the player's responsibility to fetch the key and perform decryption
    /// if the `MediaStream` implementation doesn't do it automatically.
    pub key: Option<SegmentKey>,
    /// The segment's media sequence number.
    /// This is crucial for continuity and for the internal logic of live streaming.
    pub sequence: u64,
    /// The duration of the media segment.
    pub duration: Duration,
}

/// A high-level asynchronous trait representing a consumable media stream.
///
/// This abstraction is designed to be implemented by a manager (like `HlsManager`)
/// to provide a simplified, player-friendly interface for fetching media. It hides
/// the complexity of the underlying streaming protocol (e.g., HLS).
#[async_trait]
pub trait MediaStream {
    /// Initializes the stream by fetching and parsing the master playlist.
    ///
    /// This must be called before any other methods on the stream. After a successful
    /// call, `variants()` will return the list of available streams.
    async fn init(&mut self) -> HlsResult<()>;

    /// Returns a slice of all available variants (renditions) in the stream.
    ///
    /// This should be called after initializing the stream to allow the player or
    /// an ABR (Adaptive Bitrate) controller to see what streams are available.
    /// This method is synchronous and assumes the master playlist has already been
    /// fetched and parsed during initialization of the implementing type.
    fn variants(&self) -> &[VariantStream];

    /// Selects the active variant for subsequent segment fetching.
    ///
    /// * When `next_segment` is called, it will fetch segments from the media
    /// playlist corresponding to this `variant_id`. This is the primary
    /// mechanism for an ABR controller to perform up/down-switching.
    ///
    /// # Arguments
    ///
    /// * `variant_id`: The ID of the variant to switch to. The ID must be
    ///   one of the IDs present in the slice returned by `variants()`.
    async fn select_variant(&mut self, variant_id: VariantId) -> HlsResult<()>;

    /// Fetches the next media segment from the currently selected stream.
    ///
    /// This is the core method for a player's consumption loop.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(SegmentData))`: Successfully fetched the next segment.
    /// * `Ok(None)`: The stream has ended gracefully (e.g., an HLS playlist
    ///   with an `#EXT-X-ENDLIST` tag). No more segments will be produced.
    /// * `Err(HlsError)`: An error occurred while fetching or parsing.
    ///
    /// For live streams, an implementation may block internally (asynchronously)
    /// until a new segment becomes available in the playlist.
    async fn next_segment(&mut self) -> HlsResult<Option<SegmentData>>;
}
