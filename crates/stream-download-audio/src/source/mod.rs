//! Source abstraction for `stream-download-audio`.
//!
//! Goal: HTTP and HLS must look the same to the decode pipeline.
//!
//! The pipeline consumes an **ordered** stream of `SourceMsg` items:
//! - `SourceMsg::Data(Bytes)` carries compressed/container bytes in arrival order.
//! - `SourceMsg::Control(SourceControl)` carries ordered boundaries (HLS init/segment).
//! - `SourceMsg::EndOfStream` terminates the byte stream.
//!
//! Important:
//! - Ordered correctness / deterministic tests must rely on `Control` boundaries ordered with `Data`.
//! - Manual control (e.g. HLS variant switching) is wired by the orchestration layer via downcasting;
//!   the generic source trait does not contain a command hook.
//! - The HLS implementation must derive stable identities (at least `variant`) from ordered
//!   `stream-download` controls (e.g. `StreamControl::ChunkStart.stream_key`) rather than
//!   out-of-band broadcasts.
//!
//! This module only defines the trait + protocol. Concrete sources live in submodules:
//! - `http`
//! - `hls`

use std::any::Any;
use std::io;
use std::pin::Pin;

use bytes::Bytes;
use futures_util::Stream;
use url::Url;

use crate::types::HlsChunkId;

/// Ordered control boundaries emitted by sources.
///
/// This is the *source-level* control protocol, which will later be mapped into
/// `AudioControl` in the decode pipeline.
///
/// Why a separate enum?
/// - It keeps the decode pipeline independent of `stream-download` and `stream-download-hls`
///   message shapes.
/// - It allows non-HLS sources to stay simple (they can just never emit HLS controls).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceControl {
    /// HLS init segment start.
    HlsInitStart { id: HlsChunkId },
    /// HLS init segment end.
    HlsInitEnd { id: HlsChunkId },

    /// HLS media segment start.
    HlsSegmentStart { id: HlsChunkId },
    /// HLS media segment end.
    HlsSegmentEnd { id: HlsChunkId },
}

/// An ordered message emitted by an [`AudioSource`].
#[derive(Debug, Clone)]
pub enum SourceMsg {
    /// Compressed/container bytes in arrival order.
    Data(Bytes),

    /// Ordered boundary/control message.
    Control(SourceControl),

    /// Upstream finished.
    EndOfStream,
}

/// Convenience type for boxed sources.
pub type BoxAudioSource = Box<dyn AudioSource>;

/// A source that yields ordered bytes + boundaries.
///
/// This is the only interface the decode pipeline should need.
///
/// Design:
/// - Implementations are expected to do any network I/O internally.
/// - This trait is object-safe so the pipeline can hold `Box<dyn AudioSource>`.
///
/// Downcasting:
/// - This trait extends `Any` and exposes `as_any()` so callers can downcast boxed sources when
///   they need source-specific capabilities (e.g. HLS manual variant switching wiring).
pub trait AudioSource: Any + Send + Sync + 'static {
    /// A human-readable name, used for diagnostics/logging.
    fn name(&self) -> &'static str;

    /// Returns the nominal origin URL for this source (useful for logs).
    fn url(&self) -> &Url;

    /// Create the ordered message stream.
    ///
    /// Notes:
    /// - Returning a stream lets sources implement their own concurrency model.
    /// - Cancellation is expected to be handled by dropping the stream and/or an internal token.
    fn into_stream(self: Box<Self>) -> Pin<Box<dyn Stream<Item = io::Result<SourceMsg>> + Send>>;

    /// Expose `Any` for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Expose `Any` for downcasting (mutable).
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Whether this source supports manual control (wired via downcasting).
    ///
    /// This is a convenience hint for the pipeline.
    fn supports_commands(&self) -> bool {
        false
    }
}

pub mod hls;
pub mod http;
