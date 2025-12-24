//! Streaming middleware abstractions for transforming segment byte streams.
//!
//! This module introduces a simple `StreamMiddleware` trait that can wrap a
//! stream of `Bytes` and produce a transformed stream. Typical use-cases:
//! - DRM/decryption (e.g., AES-128-CBC) applied transparently to segment data
//! - Re-chunking or alignment fixes
//! - Tag parsing/stripping
//!
//! Design goals:
//! - Minimal API surface: a single trait operating on a boxed stream type.
//! - Composability: a helper to apply a chain of middlewares.
//! - Zero-copy-friendly: keep `bytes::Bytes` as the unit of data.
//!
//! Notes:
//! - The middleware trait intentionally works with a boxed stream to keep the
//!   object-safe trait simple and ergonomic to use from `HlsStream`.
//! - All errors are mapped to `HlsError` to keep error handling consistent.

use crate::downloader::HlsByteStream;
use crate::error::HlsError;
use bytes::Bytes;

/// Callback type used to transform raw key bytes fetched from a key server
/// before they are used for decryption. This allows custom key wrapping/DRM flows.
pub type KeyProcessorCallback = dyn Fn(Bytes) -> Bytes + Send + Sync;

use aes::Aes128;
use bytes::BytesMut;
use cbc::{
    Decryptor,
    cipher::{BlockDecryptMut, KeyIvInit, block_padding::Pkcs7},
};
use futures_util::StreamExt;
use tracing::trace;

use crate::manager::StreamMiddleware;

/// AES-128-CBC middleware with streaming decryption.
///
/// Performs block-wise decryption and holds back the last block of the stream
/// to properly remove PKCS#7 padding at the end.
#[derive(Clone)]
pub struct Aes128CbcMiddleware {
    key: [u8; 16],
    iv: [u8; 16],
}

impl Aes128CbcMiddleware {
    /// Create a new AES-128-CBC middleware instance.
    pub fn new(key: [u8; 16], iv: [u8; 16]) -> Self {
        Self { key, iv }
    }

    /// Update key material.
    #[allow(unused)]
    pub fn set_key(&mut self, key: [u8; 16]) {
        self.key = key;
    }

    /// Update IV.
    #[allow(unused)]
    pub fn set_iv(&mut self, iv: [u8; 16]) {
        self.iv = iv;
    }
}

impl StreamMiddleware for Aes128CbcMiddleware {
    fn apply(&self, input: HlsByteStream) -> HlsByteStream {
        let key = self.key;
        let iv = self.iv;

        // Buffer the entire segment and decrypt once at the end using PKCS#7.
        let stream = futures_util::stream::unfold(
            (input, BytesMut::new(), false),
            move |(mut input, mut buf, finished)| async move {
                loop {
                    match input.next().await {
                        Some(Ok(chunk)) => {
                            buf.extend_from_slice(&chunk);
                            continue;
                        }
                        Some(Err(e)) => {
                            return Some((
                                Err(HlsError::io(e.to_string())),
                                (input, BytesMut::new(), true),
                            ));
                        }
                        None => {
                            if finished || buf.is_empty() {
                                return None;
                            }

                            trace!(
                                encrypted_len = buf.len(),
                                "HLS DRM: AES-128-CBC decrypt finalizing buffered segment"
                            );

                            let mut data = buf.to_vec();
                            let decryptor = Decryptor::<Aes128>::new((&key).into(), (&iv).into());
                            let result = decryptor
                                .decrypt_padded_mut::<Pkcs7>(&mut data)
                                .map_err(HlsError::aes128_cbc_decrypt_failed);

                            let out = match result {
                                Ok(plain) => {
                                    trace!(
                                        encrypted_len = buf.len(),
                                        decrypted_len = plain.len(),
                                        "HLS DRM: AES-128-CBC decrypt succeeded"
                                    );
                                    Bytes::copy_from_slice(plain)
                                }
                                Err(e) => {
                                    trace!(
                                        encrypted_len = buf.len(),
                                        error = %e,
                                        "HLS DRM: AES-128-CBC decrypt failed"
                                    );
                                    return Some((Err(e), (input, BytesMut::new(), true)));
                                }
                            };

                            return Some((Ok(out), (input, BytesMut::new(), true)));
                        }
                    }
                }
            },
        );

        stream.boxed()
    }
}
