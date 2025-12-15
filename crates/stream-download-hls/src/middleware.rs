#![allow(dead_code)]
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

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::stream::{BoxStream, StreamExt};

#[cfg(feature = "aes-decrypt")]
use aes::Aes128;
#[cfg(feature = "aes-decrypt")]
use cbc::{
    Decryptor,
    cipher::{BlockDecryptMut, KeyIvInit, block_padding::Pkcs7},
};

use crate::model::HlsError;

/// A boxed stream of HLS byte chunks.
pub type HlsByteStream = BoxStream<'static, Result<Bytes, HlsError>>;

/// Trait for transforming a stream of `Bytes` in a composable manner.
///
/// This trait is object-safe and can be used behind a `Box<dyn StreamMiddleware>`.
/// It consumes an input `HlsByteStream` and returns a new transformed stream.
pub trait StreamMiddleware: Send + Sync {
    fn apply(&self, input: HlsByteStream) -> HlsByteStream;
}

/// Apply a list of middlewares to an input stream in order.
///
/// This function consumes the input stream and returns the transformed stream.
/// If `middlewares` is empty, the input stream is returned unchanged.
///
/// Middlewares are applied left-to-right: `out = mN(...(m2(m1(input))))`
pub fn apply_middlewares(
    mut input: HlsByteStream,
    middlewares: &[Arc<dyn StreamMiddleware>],
) -> HlsByteStream {
    for mw in middlewares {
        input = mw.apply(input);
    }
    input
}

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
        #[cfg(feature = "aes-decrypt")]
        {
            let key = self.key;
            let iv = self.iv;

            // Buffer the entire segment and decrypt once at the end using PKCS#7.
            let stream = futures_util::stream::unfold(
                (input, BytesMut::new(), false),
                move |(mut input, mut buf, mut finished)| async move {
                    loop {
                        match input.next().await {
                            Some(Ok(chunk)) => {
                                buf.extend_from_slice(&chunk);
                                continue;
                            }
                            Some(Err(e)) => {
                                return Some((
                                    Err(HlsError::Io(e.to_string())),
                                    (input, BytesMut::new(), true),
                                ));
                            }
                            None => {
                                if finished {
                                    return None;
                                }
                                if buf.is_empty() {
                                    return None;
                                }

                                let mut data = buf.to_vec();
                                let decryptor =
                                    Decryptor::<Aes128>::new((&key).into(), (&iv).into());
                                let result = decryptor
                                    .decrypt_padded_mut::<Pkcs7>(&mut data)
                                    .map(|plain| Bytes::copy_from_slice(plain))
                                    .map_err(|e| {
                                        HlsError::Message(format!(
                                            "AES-128-CBC decryption failed: {e}"
                                        ))
                                    });

                                finished = true;
                                return Some((result, (input, BytesMut::new(), finished)));
                            }
                        }
                    }
                },
            );

            return stream.boxed();
        }

        #[cfg(not(feature = "aes-decrypt"))]
        {
            // No-op passthrough without AES support.
            input.map(|res| res.map(|chunk| chunk)).boxed()
        }
    }
}

/// A simple middleware that does nothing (explicit passthrough).
///
/// This can be useful for testing pipeline composition without side-effects.
#[derive(Default, Clone)]
pub struct NoopMiddleware;

impl StreamMiddleware for NoopMiddleware {
    fn apply(&self, input: HlsByteStream) -> HlsByteStream {
        input
    }
}
