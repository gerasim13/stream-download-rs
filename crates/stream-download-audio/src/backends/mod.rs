/*!
Backend module for `stream-download-audio`.

This module groups source backends that feed the decoding pipeline:
- `http`: non-seekable MediaSource wrapper for `stream-download` HTTP resources
         intended to work well with Symphonia (dev-0.6) without tail probing.

Additional backends (e.g., HLS-specific sources) can be added here in the future.
*/

pub mod http;

// Re-export HTTP backend items for convenient access as `backends::*`.
pub use http::*;
