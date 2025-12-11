//! Unified audio pipeline for `stream-download`.
//!
//! This crate exposes a unified float PCM API over HLS and HTTP backends,
//! with async resampling and optional processing hooks. The implementation
//! details live in dedicated modules; this file only wires modules and re-exports.

mod api;
mod backends;
mod pipeline;
mod stream;

#[cfg(feature = "rodio")]
mod rodio;

// Re-export useful HLS types for convenience.
pub use stream_download_hls::{
    AbrConfig, AbrController, HlsConfig, HlsManager, MediaStream, SegmentData, SelectionMode,
    VariantId, VariantStream,
};

// Public API re-exports.
pub use crate::api::{AudioOptions, AudioProcessor, AudioSpec, PlayerEvent, SampleSource};
pub use crate::stream::AudioStream;

#[cfg(feature = "rodio")]
pub use crate::rodio::RodioSourceAdapter;
