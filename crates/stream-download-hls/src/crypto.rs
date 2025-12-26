//! Encryption/decryption helpers (feature-gated).
//!
//! Provides AES-128-CBC decryption middleware and related types behind the `aes-decrypt` feature.
//! High-level usage notes live in `crates/stream-download-hls/README.md`.

use aes::Aes128;
use bytes::Bytes;
use bytes::BytesMut;
use cbc::{
    Decryptor,
    cipher::{BlockDecryptMut, KeyIvInit, block_padding::Pkcs7},
};
use futures_util::StreamExt;
use tracing::trace;

use crate::downloader::HlsByteStream;
use crate::error::HlsError;
use crate::manager::StreamMiddleware;

/// Transforms raw key bytes fetched from a key server before they are used for decryption.
pub type KeyProcessorCallback = dyn Fn(Bytes) -> Bytes + Send + Sync;

/// AES-128-CBC decrypt middleware.
///
/// Note: buffers the whole segment and decrypts once at the end to remove PKCS#7 padding.
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
