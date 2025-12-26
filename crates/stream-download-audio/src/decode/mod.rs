//! Decode-layer building blocks for `stream-download-audio`.
//!
//! This module contains the "plumbing" needed to bridge async byte sources (HTTP/HLS)
//! into a blocking decoder while preserving ordering and providing real backpressure.
//!
//! The public API of the crate is exposed via `AudioDecodeStream`; these types are
//! implementation details and may change freely.

pub(crate) mod symphonia_decoder;
