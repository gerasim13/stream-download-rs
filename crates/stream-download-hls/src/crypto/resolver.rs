//! AES-128-CBC key resolver for HLS streams.
//!
//! Provides functionality to resolve AES-128-CBC encryption keys and IVs for HLS segments.

use std::fmt::{Debug, Formatter, Result};
use std::sync::Arc;

use bytes::Bytes;
use tracing::trace;

use crate::SegmentDescriptor;
use crate::downloader::{Downloader, DownloaderExt};
use crate::error::{HlsError, HlsResult};
use crate::parser::VariantId;
use crate::parser::{EncryptionMethod, KeyInfo, SegmentKey};
use crate::settings::HlsSettings;

/// Transforms raw key bytes fetched from a key server before they are used for decryption.
pub type KeyProcessorCallback = dyn Fn(Bytes) -> Bytes + Send + Sync;

/// Resolves AES-128-CBC encryption parameters for HLS segments.
#[derive(Clone)]
pub struct AesKeyResolver {
    /// Configuration settings for HLS stream download
    config: Arc<HlsSettings>,
    /// Downloader for fetching keys (includes caching)
    downloader: Arc<dyn Downloader + Send + Sync>,
}

impl Debug for AesKeyResolver {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("AesKeyResolver")
            .field("config", &"Arc<HlsSettings>")
            .field("downloader", &"Arc<dyn Downloader>")
            .finish()
    }
}

impl AesKeyResolver {
    /// Creates a new AesKeyResolver instance.
    pub fn new(config: Arc<HlsSettings>, downloader: Arc<dyn Downloader + Send + Sync>) -> Self {
        Self { config, downloader }
    }

    /// Resolves AES-128-CBC parameters (key and IV) for a segment.
    ///
    /// Returns `Ok(Some((key, iv)))` if the segment is encrypted with AES-128-CBC,
    /// `Ok(None)` if the segment is not encrypted or uses a different encryption method,
    /// and `Err` if there was an error resolving the key.
    pub async fn resolve_aes128_cbc_params(
        &mut self,
        master_url: &str,
        key: Option<&SegmentKey>,
        variant_id: VariantId,
        sequence: Option<u64>,
    ) -> HlsResult<Option<([u8; 16], [u8; 16])>> {
        // Use pattern matching to extract all required information in one go
        let Some(SegmentKey {
            method: EncryptionMethod::Aes128,
            key_info: Some(key_info),
        }) = key
        else {
            return Ok(None);
        };

        let Some(key_uri) = &key_info.uri else {
            return Ok(None);
        };

        // Fetch and validate key
        let abs_key_url = self.resolve_url(master_url, key_uri)?;
        let final_key_url = self.finalize_key_url(&abs_key_url)?;
        let key_bytes = self.fetch_key_bytes(&final_key_url, variant_id).await?;

        let mut key_arr = [0u8; 16];
        key_arr.copy_from_slice(&key_bytes);
        let iv = Self::compute_iv(key_info, sequence);

        Ok(Some((key_arr, iv)))
    }

    /// Resolve DRM (AES-128-CBC) params for a descriptor if applicable.
    ///
    /// Includes error handling and logging specific to segment descriptors.
    /// If a segment advertises encryption but we failed to resolve key/IV,
    /// this is a hard error: continuing would stream encrypted bytes as plaintext.
    pub async fn resolve_drm_params_for_desc(
        &mut self,
        master_url: &str,
        variant_id: VariantId,
        desc: &SegmentDescriptor,
    ) -> HlsResult<Option<([u8; 16], [u8; 16])>> {
        use tracing::trace;

        trace!(
            is_init = desc.is_init,
            sequence = desc.sequence,
            variant_id = variant_id.0,
            has_key = desc.key.is_some(),
            "HLS DRM: resolving DRM params for descriptor"
        );

        let resolved_result = self
            .resolve_aes128_cbc_params(
                master_url,
                desc.key.as_ref(),
                variant_id,
                if desc.is_init {
                    None
                } else {
                    Some(desc.sequence)
                },
            )
            .await;

        match resolved_result {
            Ok(Some((key, iv))) => {
                trace!(
                    iv = ?iv,
                    "HLS DRM: resolved AES-128-CBC params for descriptor"
                );
                Ok(Some((key, iv)))
            }
            Ok(None) => {
                trace!("HLS DRM: no AES-128-CBC params resolved for descriptor");
                Ok(None)
            }
            Err(e) => {
                // If a segment advertises encryption but we failed to resolve key/IV,
                // this is a hard error: continuing would stream encrypted bytes as plaintext.
                if desc.key.is_some() {
                    tracing::error!(
                        error = %e,
                        is_init = desc.is_init,
                        sequence = desc.sequence,
                        variant_id = variant_id.0,
                        uri = %desc.uri,
                        "HLS DRM: failed to resolve AES-128 params for encrypted segment"
                    );
                    Err(e)
                } else {
                    trace!(
                        error = %e,
                        "HLS DRM: resolve_aes128_cbc_params failed (no key present in descriptor)"
                    );
                    Ok(None)
                }
            }
        }
    }

    /// Fetches key bytes, using cache if available.
    async fn fetch_key_bytes(
        &mut self,
        final_key_url: &str,
        variant_id: VariantId,
    ) -> HlsResult<Bytes> {
        // Downloader includes caching via CacheDownloader layer if configured
        let resource = crate::downloader::Resource::key(final_key_url, variant_id)?;
        let key_bytes = self.downloader.download_key(&resource, None).await?;

        let mut kb = key_bytes;
        if let Some(cb) = &self.config.key_processor_cb {
            kb = cb.as_ref()(kb);
        }

        if kb.len() != 16 {
            return Err(HlsError::invalid_aes128_key_len(kb.len()));
        }

        Ok(kb)
    }

    /// Finalizes the key URL by adding query parameters if configured.
    fn finalize_key_url(&self, abs_key_url: &str) -> HlsResult<String> {
        if let Some(params) = &self.config.key_query_params {
            let mut url = url::Url::parse(abs_key_url).map_err(HlsError::base_url_parse)?;
            {
                let mut qp = url.query_pairs_mut();
                for (k, v) in params {
                    qp.append_pair(k, v);
                }
            }
            Ok(url.to_string())
        } else {
            Ok(abs_key_url.to_string())
        }
    }

    /// Computes the IV for AES-128-CBC decryption.
    fn compute_iv(key_info: &KeyInfo, sequence: Option<u64>) -> [u8; 16] {
        if let Some(iv) = key_info.iv {
            iv
        } else if let Some(seq) = sequence {
            let mut iv = [0u8; 16];
            iv[8..].copy_from_slice(&seq.to_be_bytes());
            iv
        } else {
            [0u8; 16]
        }
    }

    /// Resolves a relative URL to an absolute URL based on the master URL.
    fn resolve_url(&self, base_url: &str, relative_url: &str) -> HlsResult<String> {
        if relative_url.starts_with("http://") || relative_url.starts_with("https://") {
            return Ok(relative_url.to_string());
        }

        let base = url::Url::parse(base_url).map_err(HlsError::base_url_parse)?;
        let joined = base
            .join(relative_url)
            .map_err(|e| HlsError::Message(format!("failed to join URL: {}", e)))?;

        Ok(joined.to_string())
    }
}
