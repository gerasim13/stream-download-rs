#![allow(dead_code)]
//! Placeholder for DRM/decryption logic.
//!
//! The previous AES-128-CBC implementation has been intentionally removed.
//! Going forward, decryption will be implemented as a streaming middleware that
//! transforms an incoming `Stream<Item = Result<bytes::Bytes, HlsError>>` of encrypted
//! data into a stream of decrypted chunks.
//!
//! Why middleware?
//! - It allows true streaming decryption (no buffering of entire segments).
//! - It composes naturally with other stream adapters (e.g., metrics, re-chunking).
//! - It avoids coupling low-level crypto with HLS state management.
//!
//! Migration plan (high-level):
//! 1) Keep key retrieval and IV computation in `HlsManager` (already in place).
//! 2) Introduce a streaming middleware layer that:
//!    - Accepts key material/IV (or a way to resolve them per segment).
//!    - Wraps the raw HTTP byte stream for each segment.
//!    - Outputs decrypted `Bytes` chunks.
//! 3) Wire the middleware chain in `HlsStream` before yielding data downstream.
//!
//! Notes:
//! - Use `bytes::Bytes` for all byte buffers (avoid `Vec<u8>` in APIs).
//! - Do not add unused “future” APIs here. Add only what’s needed by current code.
//! - Keep this module minimal; the real logic will live in the middleware layer.
//!
//! TODO:
//! - Implement a `DecryptingMiddleware` that applies AES-128-CBC as a streaming adapter.
//! - Integrate the middleware into the segment download pipeline.
//! - Add feature flags for crypto dependencies when the implementation is ready.
