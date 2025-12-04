#![allow(dead_code)]
//! Crypto helpers and placeholders for HLS AES decryption.
//!
//! This module introduces a stateful decryptor abstraction that mirrors the shape
//! of a real AES-128-CBC decryptor without forcing crypto dependencies on users
//! by default. The actual decryption is gated behind the `aes-decrypt` feature.
//!
//! When the `aes-decrypt` feature is disabled, the decryptor acts as a no-op
//! passthrough that buffers the incoming data and returns it unchanged from
//! `finalize()`. This lets higher layers wire the plumbing without pulling in
//! crypto crates prematurely.
//!
//! When the `aes-decrypt` feature is enabled, decryption is performed using
//! AES-128 in CBC mode with PKCS#7 padding via the `aes` and `cbc` crates.
//!
//! Typical usage:
//! - Construct with `Aes128CbcDecryptor::new(key, iv)`
//! - Call `update(&[u8])` for each ciphertext chunk
//! - Call `finalize()` to receive the decrypted `Bytes`
//!
//! For convenience, a helper `decrypt_aes128_cbc_full` is provided to decrypt
//! a single contiguous ciphertext buffer.

use bytes::Bytes;

use crate::model::HlsError;

/// Stateful AES-128-CBC decryptor with PKCS#7 padding.
///
/// This struct mirrors how a real decryptor would be used in a streaming context
/// and holds an internal buffer for ciphertext accumulation.
#[derive(Debug, Clone)]
pub struct Aes128CbcDecryptor {
    key: [u8; 16],
    iv: [u8; 16],
    buf: Vec<u8>,
}

impl Aes128CbcDecryptor {
    /// Creates a new decryptor with the specified key and IV.
    ///
    /// Both the key and IV must be 16 bytes (128 bits).
    pub fn new(key: [u8; 16], iv: [u8; 16]) -> Self {
        Self {
            key,
            iv,
            buf: Vec::with_capacity(1024),
        }
    }

    /// Resets the internal buffer state while keeping the key/IV.
    pub fn reset(&mut self) {
        self.buf.clear();
    }

    /// Updates the IV (e.g., if a new IV is specified for the next segment).
    pub fn set_iv(&mut self, iv: [u8; 16]) {
        self.iv = iv;
    }

    /// Updates the key material.
    pub fn set_key(&mut self, key: [u8; 16]) {
        self.key = key;
    }

    /// Appends a chunk of ciphertext to the internal buffer.
    pub fn update(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Decrypts the accumulated ciphertext and returns the plaintext bytes.
    ///
    /// Behavior depends on the `aes-decrypt` feature:
    /// - Without `aes-decrypt`: returns the buffered data unchanged.
    /// - With `aes-decrypt`: performs AES-128-CBC decryption using PKCS#7 padding.
    pub fn finalize(&mut self) -> Result<Bytes, HlsError> {
        #[cfg(feature = "aes-decrypt")]
        {
            use aes::Aes128;
            use cbc::Decryptor;
            use cbc::cipher::{BlockDecryptMut, KeyIvInit, block_padding::Pkcs7};

            // Ensure the buffer length is a multiple of the block size for CBC.
            if self.buf.is_empty() || self.buf.len() % 16 != 0 {
                return Err(HlsError::Message(format!(
                    "invalid AES-128-CBC ciphertext length: {}",
                    self.buf.len()
                )));
            }

            let mut tmp = self.buf.clone();
            let res = Decryptor::<Aes128>::new((&self.key).into(), (&self.iv).into())
                .decrypt_padded_mut::<Pkcs7>(&mut tmp)
                .map_err(|e| HlsError::Message(format!("AES decryption failed: {e}")))?;
            let out = Bytes::copy_from_slice(res);
            self.buf.clear();
            return Ok(out);
        }

        #[cfg(not(feature = "aes-decrypt"))]
        {
            // No-op passthrough: return the buffered bytes unchanged.
            let out = Bytes::from(std::mem::take(&mut self.buf));
            Ok(out)
        }
    }
}

/// Convenience function to decrypt a single contiguous ciphertext buffer.
///
/// Returns the plaintext as `Bytes`. If the `aes-decrypt` feature is disabled,
/// this will return the original `ciphertext` unchanged.
pub fn decrypt_aes128_cbc_full(
    key: &[u8],
    iv: &[u8],
    ciphertext: Bytes,
) -> Result<Bytes, HlsError> {
    if key.len() != 16 {
        return Err(HlsError::Message(format!(
            "invalid key length: expected 16, got {}",
            key.len()
        )));
    }
    if iv.len() != 16 {
        return Err(HlsError::Message(format!(
            "invalid iv length: expected 16, got {}",
            iv.len()
        )));
    }
    let mut key_arr = [0u8; 16];
    let mut iv_arr = [0u8; 16];
    key_arr.copy_from_slice(key);
    iv_arr.copy_from_slice(iv);

    let mut dec = Aes128CbcDecryptor::new(key_arr, iv_arr);
    dec.update(&ciphertext);
    dec.finalize()
}
