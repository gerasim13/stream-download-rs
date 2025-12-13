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
use tracing::instrument;

use crate::downloader::ResourceDownloader;
use crate::model::{
    EncryptionMethod, HlsConfig, HlsError, HlsResult, KeyInfo, MasterPlaylist, MediaPlaylist,
    MediaSegment, SegmentKey, VariantId, VariantStream,
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
    /// Whether init segment has been sent for the current variant.
    init_segment_sent: bool,
    /// Cache of fetched AES-128 keys by absolute URI.
    key_cache: HashMap<String, Bytes>,
    /// Cache of downloaded init segments by absolute URI.
    init_segment_cache: HashMap<String, Bytes>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct PlaylistSnapshot {
    end_list: bool,
    last_seq: u64,
    target_duration: Option<std::time::Duration>,
    current_segment: Option<MediaSegment>,
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
            init_segment_sent: false,
            key_cache: HashMap::new(),
            init_segment_cache: HashMap::new(),
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

    fn current_variant(&self) -> HlsResult<&VariantStream> {
        let master = self
            .master
            .as_ref()
            .ok_or_else(|| HlsError::Message("master not loaded; call init first".to_string()))?;

        let idx = self.current_variant_index.ok_or_else(|| {
            HlsError::Message("no variant selected; call select_variant first".to_string())
        })?;

        master
            .variants
            .get(idx)
            .ok_or_else(|| HlsError::Message("current variant index is out of bounds".to_string()))
    }

    fn current_variant_id(&self) -> HlsResult<VariantId> {
        Ok(self.current_variant()?.id)
    }

    fn current_codec_info(&self) -> HlsResult<Option<crate::model::CodecInfo>> {
        Ok(self.current_variant()?.codec.clone())
    }

    #[allow(dead_code)]
    fn current_variant_info(&self) -> HlsResult<(VariantId, Option<crate::model::CodecInfo>)> {
        let variant = self.current_variant()?;
        Ok((variant.id, variant.codec.clone()))
    }

    #[allow(dead_code)]
    fn playlist_snapshot(&self) -> HlsResult<PlaylistSnapshot> {
        let pl = self.current_media_playlist.as_ref().ok_or_else(|| {
            HlsError::Message("no media playlist loaded; call select_variant first".to_string())
        })?;

        let last_seq = pl
            .segments
            .last()
            .map(|s| s.sequence)
            .unwrap_or_else(|| pl.media_sequence.saturating_sub(1));

        let current_segment = pl.segments.get(self.next_segment_index).cloned();

        Ok(PlaylistSnapshot {
            end_list: pl.end_list,
            last_seq,
            target_duration: pl.target_duration,
            current_segment,
        })
    }

    async fn live_refresh_cycle(
        &mut self,
        last_seq: u64,
        target_duration: Option<std::time::Duration>,
    ) -> HlsResult<(Option<usize>, usize, bool, std::time::Duration)> {
        self.refresh_media_playlist().await?;

        let (found_new_idx, new_total, new_end_list) = {
            let new_pl = self
                .current_media_playlist
                .as_ref()
                .expect("playlist just refreshed");

            let found_new_idx = new_pl.segments.iter().position(|s| s.sequence > last_seq);

            (found_new_idx, new_pl.segments.len(), new_pl.end_list)
        };

        let interval = self
            .config
            .live_refresh_interval
            .or(target_duration.map(|d| d / 2))
            .unwrap_or(std::time::Duration::from_secs(2))
            .max(std::time::Duration::from_millis(500));

        Ok((found_new_idx, new_total, new_end_list, interval))
    }

    /// Return the currently cached master playlist, if any.
    async fn try_emit_init_segment(&mut self) -> HlsResult<Option<crate::traits::SegmentType>> {
        let init_opt = self
            .current_media_playlist
            .as_ref()
            .and_then(|p| p.init_segment.as_ref())
            .cloned();

        if let Some(init_segment) = init_opt {
            let (variant_id, codec_info) = self.current_variant_info()?;
            let resolved_uri = self.resolve_url(&init_segment.uri)?;

            let data = if let Some(cached) = self.init_segment_cache.get(&resolved_uri) {
                cached.clone()
            } else {
                let downloaded = bytes::Bytes::from(
                    self.download_segment(&init_segment.uri, init_segment.key.as_ref(), None)
                        .await?,
                );
                self.init_segment_cache
                    .insert(resolved_uri.clone(), downloaded.clone());
                downloaded
            };

            return Ok(Some(crate::traits::SegmentType::Init(SegmentData {
                data,
                variant_id,
                codec_info,
                key: init_segment.key,
                sequence: 0,
                duration: std::time::Duration::from_secs(0),
            })));
        }

        Ok(None)
    }

    async fn emit_media_segment(
        &mut self,
        seg: MediaSegment,
    ) -> HlsResult<crate::traits::SegmentType> {
        let codec_info = self.current_codec_info()?;

        let data = Bytes::from(
            self.download_segment(&seg.uri, seg.key.as_ref(), Some(seg.sequence))
                .await?,
        );

        self.next_segment_index += 1;

        Ok(crate::traits::SegmentType::Media(SegmentData {
            data,
            variant_id: seg.variant_id,
            codec_info,
            key: seg.key,
            sequence: seg.sequence,
            duration: seg.duration,
        }))
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
            "HlsManager: switching variant from {:?} to {} (index {:?} -> {}, segments: {})",
            self.current_variant_index,
            index,
            old_index,
            next_segment_index,
            media_playlist.segments.len()
        );

        self.current_media_playlist = Some(media_playlist);
        self.current_variant_index = Some(index);
        self.media_playlist_url = Some(media_playlist_url);
        self.next_segment_index = next_segment_index;
        self.init_segment_sent = false; // Reset init segment flag when switching variants

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
    /// Download data by URI with optional encryption key.
    /// Handles AES-128 decryption if needed.
    /// sequence is used for IV generation for media segments (None for init segments).
    fn finalize_key_url(&self, abs_key_url: &str) -> HlsResult<String> {
        if let Some(params) = &self.config.key_query_params {
            let mut url = url::Url::parse(abs_key_url).map_err(|e| HlsError::Io(e.to_string()))?;
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

    async fn fetch_key_bytes(&mut self, final_key_url: &str) -> HlsResult<Bytes> {
        if let Some(cached) = self.key_cache.get(final_key_url) {
            return Ok(cached.clone());
        }

        let mut kb = self.downloader.download_bytes(final_key_url, None).await?;
        if let Some(cb) = &self.config.key_processor_cb {
            kb = (cb)(kb);
        }

        if kb.len() != 16 {
            return Err(HlsError::Message(format!(
                "invalid AES-128 key length: expected 16, got {}",
                kb.len()
            )));
        }

        self.key_cache.insert(final_key_url.to_string(), kb.clone());

        Ok(kb)
    }

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

    async fn decrypt_segment_if_needed(
        &mut self,
        data: Bytes,
        key: Option<&SegmentKey>,
        sequence: Option<u64>,
    ) -> HlsResult<Bytes> {
        if let Some(seg_key) = key {
            if matches!(seg_key.method, EncryptionMethod::Aes128) {
                if let Some(ref key_info) = seg_key.key_info {
                    if let Some(key_uri) = &key_info.uri {
                        let abs_key_url = self.resolve_url(key_uri)?;
                        let final_key_url = self.finalize_key_url(&abs_key_url)?;
                        let key_bytes = self.fetch_key_bytes(&final_key_url).await?;
                        let iv = Self::compute_iv(key_info, sequence);
                        let decrypted =
                            crate::crypto::decrypt_aes128_cbc_full(key_bytes.as_ref(), &iv, data)?;
                        return Ok(decrypted);
                    }
                }
            }
        }

        Ok(data)
    }

    async fn download_segment(
        &mut self,
        uri: &str,
        key: Option<&SegmentKey>,
        sequence: Option<u64>,
    ) -> HlsResult<Vec<u8>> {
        let resolved_url = self.resolve_url(uri)?;
        let data = self.downloader.download_bytes(&resolved_url, None).await?;
        let data = self.decrypt_segment_if_needed(data, key, sequence).await?;
        Ok(data.to_vec())
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

    #[instrument(skip(self), fields(variant_index = ?self.current_variant_index, next_segment_index = self.next_segment_index))]
    async fn next_segment(&mut self) -> HlsResult<Option<crate::traits::SegmentType>> {
        loop {
            // --- Phase 0: ensure we have a playlist selected ---
            let PlaylistSnapshot {
                end_list,
                last_seq,
                target_duration,
                current_segment: seg_opt,
            } = self.playlist_snapshot()?;

            // --- Phase 1: init segment (at most once per variant selection) ---
            if !self.init_segment_sent
                && let Some(init_seg) = self.try_emit_init_segment().await?
            {
                // Mark even if absent to avoid re-checking on subsequent calls
                self.init_segment_sent = true;
                return Ok(Some(init_seg));
            }

            // --- Phase 2: media segment available right now ---
            if let Some(seg) = seg_opt {
                return Ok(Some(self.emit_media_segment(seg).await?));
            }

            // --- Phase 3: no segment at current index ---
            if end_list {
                tracing::trace!(
                    "HlsManager: returning None (EOF). end_list=true last_seq={} next_segment_index={}",
                    last_seq,
                    self.next_segment_index
                );
                return Ok(None);
            }

            // --- Phase 4: LIVE state machine (refresh -> find new segment -> sleep) ---
            let (found_new_idx, new_total, new_end_list, interval) =
                self.live_refresh_cycle(last_seq, target_duration).await?;

            if let Some(idx) = found_new_idx {
                tracing::trace!(
                    "HlsManager: LIVE refresh produced new segment: last_seq={} first_new_idx={}",
                    last_seq,
                    idx
                );
                self.next_segment_index = idx;
                continue;
            }

            // If the stream ended between refreshes, the next loop iteration will return EOF.
            if new_end_list {
                tracing::trace!(
                    "HlsManager: LIVE refresh indicates end_list=true (no new segments). last_seq={} total_segments={}",
                    last_seq,
                    new_total
                );
            }

            tracing::trace!(
                "HlsManager: LIVE no new segments yet. last_seq={} total_segments={} sleep={:?}",
                last_seq,
                new_total,
                interval
            );

            tokio::time::sleep(interval).await;
        }
    }
}
