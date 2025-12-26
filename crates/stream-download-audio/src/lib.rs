//! `stream-download-audio` — iterator-style (async `Stream`) audio decoding for HTTP and HLS.
//!
//! # Goals (no legacy, test-first)
//! - Expose a single **ordered** stream of decoded PCM + control boundaries (like `stream-download-hls`).
//! - Keep out-of-band events optional; deterministic testing must rely on ordered messages.
//! - Prefer clear backpressure semantics via bounded channels/queues.
//! - Avoid "mysterious fixed buffers": capacities are expressed in meaningful units.
//!
//! # High-level model
//! Consumers construct a decoder stream (HTTP or HLS) and iterate it:
//!
//! - `AudioDecodeStream::new_http(url, opts)`
//! - `AudioDecodeStream::new_hls(url, hls_settings, opts, storage_root)`
//!
//! The stream yields `AudioMsg` items:
//! - `AudioMsg::Control(AudioControl::...)`
//! - `AudioMsg::Pcm(PcmChunk)`
//!
//! The ordered controls are intended for strict tests:
//! - `HlsInitStart/End`
//! - `HlsSegmentStart/End`
//! - `FormatChanged`
//! - `EndOfStream`
//!
//! Notes:
//! - This crate is not production-stable yet. The API is intentionally small and may change.
//! - There is no legacy pull API.
//!
//! Implementation modules are private and may be refactored freely.

#![forbid(unsafe_code)]

mod decode;
mod source;
mod stream;
mod types;

// Optional rodio adapter (feature-gated).
#[cfg(feature = "rodio")]
mod rodio;

pub use crate::stream::{AudioDecodeStream, AudioSource};
pub use crate::types::{
    AudioCommand, AudioControl, AudioDecodeOptions, AudioMsg, AudioSpec, DecoderLifecycleReason,
    HlsChunkId, PcmChunk,
};

/// Re-export commonly used ecosystem types so downstream code doesn't need to depend on them
/// directly (optional convenience).
pub use url::Url;

pub use stream_download_hls::HlsSettings;

/// Optional rodio adapter for `AudioDecodeStream`.
#[cfg(feature = "rodio")]
pub use crate::rodio::RodioSourceAdapter;
