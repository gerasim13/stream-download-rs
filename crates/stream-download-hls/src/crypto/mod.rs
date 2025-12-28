//! Encryption/decryption helpers (feature-gated).
//!
//! Provides AES-128-CBC decryption middleware and related types behind the `aes-decrypt` feature.
//! High-level usage notes live in `crates/stream-download-hls/README.md`.

#[cfg(feature = "aes-decrypt")]
pub mod middleware;

#[cfg(feature = "aes-decrypt")]
pub mod resolver;
