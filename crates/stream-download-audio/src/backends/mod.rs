/*!
Backend module for `stream-download-audio`.

This module groups source backends that feed the decoding pipeline:
- `http`: seek-gated MediaSource wrapper for `stream-download` HTTP resources, designed
          to avoid tail-probing during format detection and enable seeking post-probe.
- `hls`:  streaming MediaSource that feeds init + media segments from `HlsManager` to
          Symphonia in a blocking fashion (non-seekable for now).

Additional backends (e.g., S3/opendal, process, async_read) can be added here in the future.
*/

pub mod hls;
pub mod http;

// Re-export backend items for convenient access as `backends::*`.
pub use hls::*;
pub use http::*;
