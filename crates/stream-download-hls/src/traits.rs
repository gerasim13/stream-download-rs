//! High-level asynchronous traits for consuming media streams.
//!
//! This module defines the `MediaStream` trait, which provides a protocol-agnostic
//! contract for a player to interact with a streaming source like HLS. The goal
//! is to abstract away the complexity of playlist parsing, live updates, and
//! variant switching, offering a simple `next_segment` iterator-like API.
//!
//! # Non-blocking Design
//!
//! The trait is designed to avoid blocking in hot paths. For live streams that
//! need to wait for new segments, [`MediaStream::next_segment_nonblocking`]
//! returns [`NextSegmentResult::NeedsRefresh`] with a suggested wait duration,
//! allowing the caller to handle waiting with proper cancellation support.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::model::{CodecInfo, HlsByteStream, HlsResult, SegmentKey, VariantId, VariantStream};

/// Type of segment returned by `next_segment`.
#[derive(Debug, Clone)]
pub enum SegmentType {
    /// Initialization segment containing container headers and metadata.
    /// Must be processed before any media segments from the same variant.
    Init(SegmentData),
    /// Media segment containing actual audio/video data.
    Media(SegmentData),
}

/// Result of a non-blocking segment fetch attempt.
///
/// This enum allows the caller to handle waiting for live streams
/// with proper cancellation support, rather than blocking inside
/// the `next_segment` implementation.
#[derive(Debug, Clone)]
pub enum NextSegmentResult {
    /// A segment is available and ready to be processed.
    Segment(SegmentType),

    /// The stream has ended (VOD finished or live stream with EXT-X-ENDLIST).
    /// No more segments will be produced.
    EndOfStream,

    /// Live stream has no new segments yet.
    ///
    /// The caller should wait for the specified duration before calling
    /// `next_segment_nonblocking` again. This allows the caller to use
    /// `tokio::select!` for proper cancellation handling.
    NeedsRefresh {
        /// Suggested wait duration before the next refresh attempt.
        /// Typically based on the playlist's target duration.
        wait: Duration,
    },
}

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
///
/// # Blocking vs Non-blocking
///
/// The trait provides two methods for fetching segments:
///
/// - [`next_segment`](Self::next_segment): Blocking version that waits internally
///   for live streams. Simpler to use but harder to cancel.
///
/// - [`next_segment_nonblocking`](Self::next_segment_nonblocking): Non-blocking
///   version that returns immediately with [`NextSegmentResult::NeedsRefresh`]
///   if no segment is available. The caller is responsible for waiting.
///
/// For new code, prefer `next_segment_nonblocking` as it allows proper
/// cancellation handling via `tokio::select!`.
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

    /// Fetches the next segment from the currently selected stream (blocking).
    ///
    /// This is the core method for a player's consumption loop.
    /// Returns either an initialization segment or a media segment.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(SegmentType::Init(...)))`: Initialization segment.
    /// * `Ok(Some(SegmentType::Media(...)))`: Media segment.
    /// * `Ok(None)`: The stream has ended gracefully (e.g., an HLS playlist
    ///   with an `#EXT-X-ENDLIST` tag). No more segments will be produced.
    /// * `Err(HlsError)`: An error occurred while fetching or parsing.
    ///
    /// For live streams, this method blocks (asynchronously) until a new
    /// segment becomes available. For better cancellation support, use
    /// [`next_segment_nonblocking`](Self::next_segment_nonblocking).
    async fn next_segment(&mut self) -> HlsResult<Option<SegmentType>>;

    /// Fetches the next segment without blocking for live streams.
    ///
    /// Unlike [`next_segment`](Self::next_segment), this method returns
    /// immediately if no segment is available for live streams, allowing
    /// the caller to handle waiting with proper cancellation support.
    ///
    /// # Returns
    ///
    /// * `Ok(NextSegmentResult::Segment(...))`: A segment is ready.
    /// * `Ok(NextSegmentResult::EndOfStream)`: Stream has ended.
    /// * `Ok(NextSegmentResult::NeedsRefresh { wait })`: No segment available,
    ///   caller should wait `wait` duration before retrying.
    /// * `Err(HlsError)`: An error occurred.
    ///
    /// # Example
    ///
    /// ```ignore
    /// loop {
    ///     tokio::select! {
    ///         _ = cancel_token.cancelled() => break,
    ///         result = stream.next_segment_nonblocking() => {
    ///             match result? {
    ///                 NextSegmentResult::Segment(seg) => process(seg),
    ///                 NextSegmentResult::EndOfStream => break,
    ///                 NextSegmentResult::NeedsRefresh { wait } => {
    ///                     tokio::select! {
    ///                         _ = cancel_token.cancelled() => break,
    ///                         _ = tokio::time::sleep(wait) => continue,
    ///                     }
    ///                 }
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    async fn next_segment_nonblocking(&mut self) -> HlsResult<NextSegmentResult>;
}

/// Trait for transforming a stream of `Bytes` in a composable manner.
///
/// This trait is object-safe and can be used behind a `Box<dyn StreamMiddleware>`.
/// It consumes an input `HlsByteStream` and returns a new transformed stream.
pub trait StreamMiddleware: Send + Sync {
    fn apply(&self, input: HlsByteStream) -> HlsByteStream;
}

/// Apply a list of middlewares to an input stream in order.
///
/// This function consumes the input stream and returns the transformed stream.
/// If `middlewares` is empty, the input stream is returned unchanged.
///
/// Middlewares are applied left-to-right: `out = mN(...(m2(m1(input))))`
pub fn apply_middlewares(
    mut input: HlsByteStream,
    middlewares: &[Arc<dyn StreamMiddleware>],
) -> HlsByteStream {
    for mw in middlewares {
        input = mw.apply(input);
    }
    input
}
