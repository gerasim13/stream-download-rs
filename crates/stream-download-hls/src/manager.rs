//! HLS stream manager.
//!
//! This module provides [`HlsManager`], a high-level handle for working with a
//! single HLS stream (identified by a master playlist URL).
//!
//! At this stage it intentionally contains only a minimal, non-networked
//! skeleton so that higher layers can start integrating against a stable API.
//! Network I/O and real HLS behavior (parsing, live updates, segment fetching)
//! will be wired in future iterations using the other modules in this crate.
//!
//! Responsibilities of `HlsManager` in the PoC design:
//! - Own the master playlist URL and configuration.
//! - Coordinate parsing and refreshing of master/media playlists (later).
//! - Expose a small async API for upper layers (ABR controller, player) to:
//!   - load the master playlist and list variants,
//!   - select/switch the current variant,
//!   - poll new segments and download their bytes.
//!
//! For now, all async methods are stubs that return `HlsError::Message`
//! so that the interface compiles and can be used, while the internal
//! implementation is developed incrementally.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;

use crate::downloader::ResourceDownloader;
use crate::model::{
    EncryptionMethod, HlsConfig, HlsError, HlsResult, MasterPlaylist, MediaPlaylist, MediaSegment,
    VariantId, VariantStream,
};
use crate::parser::{parse_master_playlist, parse_media_playlist};
use crate::traits::{MediaStream, SegmentData};

/// High-level handle for working with a single HLS stream.
///
/// The manager is created with a master playlist URL and configuration.
/// It does not perform any network I/O until its async methods are called.
///
/// In its final form, `HlsManager` will:
/// - use [`ResourceDownloader`] to fetch playlists and segments via
///   `stream-download`,
/// - maintain internal state for the selected variant and latest media
///   playlist,
/// - provide small, composable building blocks for an ABR controller
///   and player-level buffering logic.
///
/// At the moment, most methods are stubs to allow early integration.
#[derive(Debug)]
pub struct HlsManager {
    /// URL of the master playlist.
    master_url: String,
    /// Configuration parameters.
    config: HlsConfig,
    /// Downloader used to fetch playlists and segments.
    downloader: ResourceDownloader,

    /// Cached master playlist, once loaded.
    master: Option<MasterPlaylist>,
    /// Index of the currently selected variant in `master.variants`.
    current_variant_index: Option<usize>,
    /// Cached media playlist for the current variant.
    current_media_playlist: Option<MediaPlaylist>,

    /// Cached media playlist URL for the current variant.
    media_playlist_url: Option<String>,
    /// Next segment index in the current media playlist.
    next_segment_index: usize,
    /// Cache of fetched AES-128 keys by absolute URI.
    key_cache: HashMap<String, Bytes>,
}

impl HlsManager {
    /// Create a new `HlsManager` for the given master playlist URL.
    ///
    /// This constructor is synchronous and does not perform any network I/O.
    /// The `downloader` is injected so that callers can customize how
    /// HTTP/caching is configured (e.g., shared client, different backends).
    pub fn new(
        master_url: impl Into<String>,
        config: HlsConfig,
        downloader: ResourceDownloader,
    ) -> Self {
        Self {
            master_url: master_url.into(),
            config,
            downloader,
            master: None,
            current_variant_index: None,
            current_media_playlist: None,
            media_playlist_url: None,
            next_segment_index: 0,
            key_cache: HashMap::new(),
        }
    }

    /// Get the master playlist URL associated with this manager.
    pub fn master_url(&self) -> &str {
        &self.master_url
    }

    /// Access the configuration used by this manager.
    pub fn config(&self) -> &HlsConfig {
        &self.config
    }

    /// Access the underlying downloader.
    ///
    /// This can be useful for testing or advanced customization at higher
    /// layers, but typical consumers should not need it.
    pub fn downloader(&self) -> &ResourceDownloader {
        &self.downloader
    }

    /// Mutable access to the underlying downloader.
    pub fn downloader_mut(&mut self) -> &mut ResourceDownloader {
        &mut self.downloader
    }

    fn resolve_url(&self, relative_url: &str) -> HlsResult<String> {
        let base_url = if let Some(ref media_playlist_url) = self.media_playlist_url {
            media_playlist_url.as_str()
        } else {
            self.master_url.as_str()
        };

        let base = url::Url::parse(base_url)
            .map_err(|e| HlsError::Io(format!("Failed to parse base URL: {}", e)))?;
        base.join(relative_url)
            .map(|u| u.into())
            .map_err(|e| HlsError::Io(format!("Failed to join URL: {}", e)))
    }

    /// Return the currently cached master playlist, if any.
    pub fn master(&self) -> Option<&MasterPlaylist> {
        self.master.as_ref()
    }

    /// Return the index of the currently selected variant, if any.
    pub fn current_variant_index(&self) -> Option<usize> {
        self.current_variant_index
    }

    /// Return the currently cached media playlist for the selected variant.
    pub fn current_media_playlist(&self) -> Option<&MediaPlaylist> {
        self.current_media_playlist.as_ref()
    }

    /// Load and parse the master playlist.
    ///
    /// In the final implementation this will:
    /// - download the master playlist bytes via `ResourceDownloader`,
    /// - parse them with `parse_master_playlist`,
    /// - cache the result in `self.master`.
    ///
    /// For now this is a stub that always returns an error.
    pub async fn load_master(&mut self) -> HlsResult<&MasterPlaylist> {
        let data = self
            .downloader
            .download_bytes(&self.master_url, None)
            .await?;
        let master_playlist = parse_master_playlist(&data)?;
        self.master = Some(master_playlist);
        Ok(self.master.as_ref().unwrap())
    }

    /// Select a variant by index.
    ///
    /// The typical flow will be:
    /// - call `load_master` to get the list of variants,
    /// - choose an index (e.g., by bandwidth),
    /// - call `select_variant(index)`.
    ///
    /// In the final implementation this will also:
    /// - resolve the media playlist URI for the selected variant,
    /// - download and parse the media playlist,
    /// - initialize `current_media_playlist`.
    ///
    /// For now this is a stub that only validates input against a cached
    /// master playlist (if any) and otherwise returns an error.
    pub async fn select_variant(&mut self, index: usize) -> HlsResult<()> {
        let variant = self
            .master
            .as_ref()
            .and_then(|m| m.variants.get(index))
            .ok_or_else(|| {
                HlsError::Message("variant index out of bounds or master not loaded".to_string())
            })?
            .clone(); // Clone to avoid borrowing issues

        let media_playlist_url = self.resolve_url(&variant.uri)?;
        let data = self
            .downloader
            .download_bytes(&media_playlist_url, None)
            .await?;

        let media_playlist = parse_media_playlist(&data, variant.id)?;

        // Preserve playback position by keeping the same segment index
        // This assumes variants are time-synchronized (standard HLS behavior)
        // Ensure we don't exceed the bounds of the new playlist
        let old_index = self.next_segment_index;
        let next_segment_index =
            std::cmp::min(self.next_segment_index, media_playlist.segments.len());

        tracing::debug!(
            "HlsManager: switching variant from {} to {} (index {} -> {}, segments: {})",
            self.current_variant_index.unwrap_or(usize::MAX),
            index,
            old_index,
            next_segment_index,
            media_playlist.segments.len()
        );

        self.current_media_playlist = Some(media_playlist);
        self.current_variant_index = Some(index);
        self.media_playlist_url = Some(media_playlist_url);
        self.next_segment_index = next_segment_index;

        Ok(())
    }

    /// Switch to another variant (wrapper around `select_variant`).
    ///
    /// This method is intended to be used by an ABR controller.
    pub async fn switch_variant(&mut self, new_index: usize) -> HlsResult<()> {
        self.select_variant(new_index).await
    }

    /// Refresh the media playlist for the currently selected variant and
    /// return the updated playlist.
    ///
    /// In the final implementation this will:
    /// - download the latest media playlist,
    /// - parse it with `parse_media_playlist`,
    /// - update `current_media_playlist`,
    /// - be used by higher layers to compute new segments (via `diff_playlists`).
    ///
    /// For now this is a stub that returns an error.
    pub async fn refresh_media_playlist(&mut self) -> HlsResult<&MediaPlaylist> {
        // Ensure we have a selected variant and a media playlist URL to refresh
        let media_url = self.media_playlist_url.clone().ok_or_else(|| {
            HlsError::Message("no media playlist URL; call select_variant first".to_string())
        })?;
        let variant_id = self
            .current_variant_index
            .and_then(|idx| {
                self.master
                    .as_ref()
                    .and_then(|m| m.variants.get(idx))
                    .map(|v| v.id)
            })
            .ok_or_else(|| HlsError::Message("no variant selected".to_string()))?;

        // Download and parse the latest media playlist
        let data = self.downloader.download_bytes(&media_url, None).await?;
        let media_playlist = parse_media_playlist(&data, variant_id)?;
        self.current_media_playlist = Some(media_playlist);

        Ok(self.current_media_playlist.as_ref().unwrap())
    }

    /// Download the bytes for a specific segment.
    ///
    /// This will eventually:
    /// - resolve the segment URI relative to the media playlist URL if needed,
    /// - use `ResourceDownloader` to fetch the segment bytes (possibly with
    ///   caching),
    /// - return the raw bytes to the caller for decoding.
    ///
    /// Download the bytes for a specific segment.
    ///
    /// This will:
    /// - resolve the segment URI relative to the media playlist URL if needed,
    /// - use `ResourceDownloader` to fetch the segment bytes (possibly with
    ///   caching),
    /// - handle AES-128 decryption when applicable,
    /// - return the raw bytes to the caller.
    pub async fn download_segment(&mut self, segment: &MediaSegment) -> HlsResult<Vec<u8>> {
        // Resolve segment URL relative to media playlist
        let seg_url = self.resolve_url(&segment.uri)?;
        // Download segment bytes
        let mut data = self.downloader.download_bytes(&seg_url, None).await?;

        // If encrypted with AES-128 and key URI is present, fetch key and (optionally) decrypt.
        if let Some(seg_key) = &segment.key {
            if matches!(seg_key.method, EncryptionMethod::Aes128) {
                if let Some(ref key_info) = seg_key.key_info {
                    if let Some(ref key_uri) = key_info.uri {
                        // Resolve key URL relative to media playlist URL
                        let abs_key_url = self.resolve_url(key_uri)?;

                        // Apply query params if configured
                        let final_key_url = if let Some(params) = &self.config.key_query_params {
                            let mut url = url::Url::parse(&abs_key_url)
                                .map_err(|e| HlsError::Io(e.to_string()))?;
                            {
                                let mut qp = url.query_pairs_mut();
                                for (k, v) in params {
                                    qp.append_pair(k, v);
                                }
                            }
                            url.to_string()
                        } else {
                            abs_key_url
                        };

                        // Fetch key (with cache)
                        let key_bytes = if let Some(cached) = self.key_cache.get(&final_key_url) {
                            cached.clone()
                        } else {
                            let mut kb =
                                self.downloader.download_bytes(&final_key_url, None).await?;
                            // Process key via callback if provided
                            if let Some(cb) = &self.config.key_processor_cb {
                                kb = (cb)(kb);
                            }
                            if kb.len() != 16 {
                                return Err(HlsError::Message(format!(
                                    "invalid AES-128 key length: expected 16, got {}",
                                    kb.len()
                                )));
                            }
                            self.key_cache.insert(final_key_url.clone(), kb.clone());
                            kb
                        };

                        // Compute IV: prefer explicit IV, else derive from sequence per HLS spec
                        let iv = if let Some(iv) = key_info.iv {
                            iv
                        } else {
                            let mut iv = [0u8; 16];
                            iv[8..].copy_from_slice(&segment.sequence.to_be_bytes());
                            iv
                        };

                        data =
                            crate::crypto::decrypt_aes128_cbc_full(key_bytes.as_ref(), &iv, data)?;
                    }
                }
            }
        }

        Ok(data.to_vec())
    }

    /// Download the initialization segment (EXT-X-MAP) for the current variant, if present.
    ///
    /// The bytes are decrypted if the init segment is encrypted with AES-128 and a key is available.
    /// Returns:
    /// - Ok(Some(Vec<u8>)) when an init segment exists and was fetched (decrypted if necessary)
    /// - Ok(None) when no init segment is present in the current media playlist
    pub async fn download_init_segment(&mut self) -> HlsResult<Option<Vec<u8>>> {
        let playlist = match &self.current_media_playlist {
            Some(p) => p,
            None => {
                return Err(HlsError::Message(
                    "no media playlist loaded; call select_variant first".to_string(),
                ));
            }
        };

        let init = match &playlist.init_segment {
            Some(i) => i,
            None => return Ok(None),
        };

        // Resolve init segment URL relative to the media playlist URL (or master as fallback)
        let init_url = self.resolve_url(&init.uri)?;
        let mut data = self.downloader.download_bytes(&init_url, None).await?;

        // If the init segment has encryption info, handle AES-128 decryption
        if let Some(seg_key) = &init.key {
            if matches!(seg_key.method, EncryptionMethod::Aes128) {
                if let Some(ref key_info) = seg_key.key_info {
                    if let Some(ref key_uri) = key_info.uri {
                        // Resolve key URL relative to media playlist URL
                        let abs_key_url = self.resolve_url(key_uri)?;

                        // Apply optional query params from config
                        let final_key_url = if let Some(params) = &self.config.key_query_params {
                            let mut url = url::Url::parse(&abs_key_url)
                                .map_err(|e| HlsError::Io(e.to_string()))?;
                            {
                                let mut qp = url.query_pairs_mut();
                                for (k, v) in params {
                                    qp.append_pair(k, v);
                                }
                            }
                            url.to_string()
                        } else {
                            abs_key_url
                        };

                        // Fetch or reuse cached key bytes
                        let key_bytes = if let Some(cached) = self.key_cache.get(&final_key_url) {
                            cached.clone()
                        } else {
                            let mut kb =
                                self.downloader.download_bytes(&final_key_url, None).await?;
                            if let Some(cb) = &self.config.key_processor_cb {
                                kb = (cb)(kb);
                            }
                            if kb.len() != 16 {
                                return Err(HlsError::Message(format!(
                                    "invalid AES-128 key length: expected 16, got {}",
                                    kb.len()
                                )));
                            }
                            self.key_cache.insert(final_key_url.clone(), kb.clone());
                            kb
                        };

                        // Compute IV: use explicit IV if provided; otherwise, use zero IV for init segments
                        let iv = if let Some(iv) = key_info.iv {
                            iv
                        } else {
                            [0u8; 16]
                        };

                        data =
                            crate::crypto::decrypt_aes128_cbc_full(key_bytes.as_ref(), &iv, data)?;
                    }
                }
            }
        }

        Ok(Some(data.to_vec()))
    }
}

#[async_trait]
impl MediaStream for HlsManager {
    async fn init(&mut self) -> HlsResult<()> {
        self.load_master().await?;
        Ok(())
    }

    fn variants(&self) -> &[VariantStream] {
        self.master()
            .map(|m| m.variants.as_slice())
            .unwrap_or_default()
    }

    async fn select_variant(&mut self, variant_id: VariantId) -> HlsResult<()> {
        self.select_variant(variant_id.0).await
    }

    async fn next_segment(&mut self) -> HlsResult<Option<SegmentData>> {
        loop {
            // Ensure media playlist is loaded
            let playlist = match &self.current_media_playlist {
                Some(p) => p,
                None => {
                    return Err(HlsError::Message(
                        "no media playlist loaded; call select_variant first".to_string(),
                    ));
                }
            };

            // If we have a segment ready in the current playlist, fetch it
            if self.next_segment_index < playlist.segments.len() {
                let seg = playlist.segments[self.next_segment_index].clone();

                // Download segment bytes (handles decryption if needed)
                let data = Bytes::from(self.download_segment(&seg).await?);

                // Prepare codec info from the selected variant
                let codec_info = self
                    .master
                    .as_ref()
                    .and_then(|m| {
                        self.current_variant_index
                            .and_then(|idx| m.variants.get(idx))
                    })
                    .and_then(|v| v.codec.clone());

                let segment_data = SegmentData {
                    data,
                    variant_id: seg.variant_id,
                    codec_info,
                    key: seg.key.clone(),
                    sequence: seg.sequence,
                    duration: seg.duration,
                };

                // Advance to the next segment
                self.next_segment_index += 1;

                return Ok(Some(segment_data));
            }

            // VOD end-of-stream
            if playlist.end_list {
                tracing::debug!(
                    "HlsManager: returning None (EOF). end_list=true media_sequence={} last_seg_seq={:?} total_segments={}",
                    playlist.media_sequence,
                    playlist.segments.last().map(|s| s.sequence),
                    playlist.segments.len()
                );
                return Ok(None);
            }

            // LIVE: need to refresh until new segments appear
            let last_seq = playlist
                .segments
                .last()
                .map(|s| s.sequence)
                .unwrap_or_else(|| playlist.media_sequence.saturating_sub(1));

            // Refresh immediately
            self.refresh_media_playlist().await?;
            let new_pl = self
                .current_media_playlist
                .as_ref()
                .expect("playlist just refreshed");
            tracing::debug!(
                "HlsManager: refreshed media playlist. media_sequence={} last_seq={} total_segments={}",
                new_pl.media_sequence,
                last_seq,
                new_pl.segments.len()
            );

            // Try to find the first truly new segment (sequence greater than last_seq)
            if let Some(idx) = new_pl.segments.iter().position(|s| s.sequence > last_seq) {
                tracing::debug!(
                    "HlsManager: found new segment after refresh: last_seq={} first_new_idx={} first_new_seq={}",
                    last_seq,
                    idx,
                    new_pl.segments[idx].sequence
                );
                self.next_segment_index = idx;
                // Loop will try again and fetch this segment
                continue;
            } else {
                tracing::debug!(
                    "HlsManager: no new segments after refresh. last_seq={} total_segments={} end_list={}",
                    last_seq,
                    new_pl.segments.len(),
                    new_pl.end_list
                );
            }

            // No new segments yet; wait and try again
            let interval = self
                .config
                .live_refresh_interval
                .or(new_pl.target_duration)
                .unwrap_or(std::time::Duration::from_secs(2));
            tokio::time::sleep(interval).await;
        }
    }
}
