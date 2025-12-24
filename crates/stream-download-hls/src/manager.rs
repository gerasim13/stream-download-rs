//! HLS stream manager.
//!
//! This module provides the player-facing API (`MediaStream`) plus segment iteration types.
//! Playlist/key caching is handled via `CachedResourceDownloader` and `StreamControl::StoreResource`.
//! For higher-level design notes, see `crates/stream-download-hls/README.md`.

use async_trait::async_trait;
use bytes::Bytes;
use lru::LruCache;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;

use crate::cache::keys::{self as cache_keys, master_hash_from_url};
use crate::downloader::HlsByteStream;
use crate::downloader::ResourceDownloader;
use crate::downloader::{CacheSource, CachedResourceDownloader};
use crate::error::{HlsError, HlsResult};
use crate::parser::{
    CodecInfo, EncryptionMethod, InitSegment, KeyInfo, MasterPlaylist, MediaPlaylist, MediaSegment,
    SegmentKey, VariantId, VariantStream, parse_master_playlist, parse_media_playlist,
};
use crate::settings::HlsSettings;
use stream_download::source::{StreamControl, StreamMsg};
use stream_download::storage::StorageHandle;
use tokio::sync::mpsc;
use tracing::trace;

/// Player-facing interface for iterating HLS segments.
/// Use [`Self::next_segment`] for blocking behavior, or [`Self::next_segment_nonblocking`] for polling.
#[async_trait]
pub trait MediaStream {
    /// Initializes the stream by fetching and parsing the master playlist.
    async fn init(&mut self) -> HlsResult<()>;

    /// Returns a slice of all available variants (renditions) in the stream.
    fn variants(&self) -> &[VariantStream];

    /// Selects the active variant for subsequent segment fetching.
    async fn select_variant(&mut self, variant_id: VariantId) -> HlsResult<()>;

    /// Returns the next segment (blocks for live until one becomes available).
    ///
    /// For cancellation-friendly polling, use [`Self::next_segment_nonblocking`].
    async fn next_segment(&mut self) -> HlsResult<Option<SegmentType>>;

    /// Fetches the next segment without blocking for live streams.
    async fn next_segment_nonblocking(&mut self) -> HlsResult<NextSegmentResult>;
}

/// Transforms an `HlsByteStream` (object-safe).
pub trait StreamMiddleware: Send + Sync {
    fn apply(&self, input: HlsByteStream) -> HlsByteStream;
}

/// Applies middlewares left-to-right (no-op if `middlewares` is empty).
pub fn apply_middlewares(
    mut input: HlsByteStream,
    middlewares: &[Arc<dyn StreamMiddleware>],
) -> HlsByteStream {
    for mw in middlewares {
        input = mw.apply(input);
    }
    input
}

/// A segment yielded by the stream.
#[derive(Debug, Clone)]
pub enum SegmentType {
    /// Initialization segment (if present, precedes media segments for the same variant).
    Init(SegmentData),
    /// Media segment containing actual audio/video data.
    Media(SegmentData),
}

/// Result of a non-blocking segment fetch attempt.
#[derive(Debug, Clone)]
pub enum NextSegmentResult {
    /// A segment is available and ready to be processed.
    Segment(SegmentType),

    /// End of stream (VOD finished or live playlist has `#EXT-X-ENDLIST`).
    EndOfStream,

    /// Live stream has no new segments yet.
    NeedsRefresh {
        /// Suggested wait duration before the next refresh attempt.
        wait: Duration,
    },
}

/// Raw segment bytes plus metadata required for decoding/scheduling.
#[derive(Debug, Clone)]
pub struct SegmentData {
    /// Raw bytes of the media segment (e.g., a TS or fMP4 file).
    pub data: Bytes,
    /// Uniquely identifies the variant (stream/rendition) this segment belongs to.
    pub variant_id: VariantId,
    /// Codec and container information for the associated variant.
    pub codec_info: Option<CodecInfo>,
    /// Encryption key information, if the segment is encrypted.
    pub key: Option<SegmentKey>,
    /// The segment's media sequence number.
    pub sequence: u64,
    /// The duration of the media segment.
    pub duration: Duration,
}

/// High-level handle for a single HLS stream (no network I/O until async methods are called).
#[derive(Debug)]
pub struct HlsManager {
    /// URL of the master playlist.
    master_url: url::Url,
    /// Configuration parameters.
    config: Arc<HlsSettings>,
    /// Downloader used to fetch playlists and segments.
    downloader: ResourceDownloader,
    /// Cached wrapper for playlist/key reads (read-before-fetch using `StorageHandle`).
    cached_downloader: CachedResourceDownloader,
    /// Control sender used to persist fetched resources via `StoreResource`.
    control_sender: mpsc::Sender<StreamMsg>,

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
    /// Cache of downloaded init segments by absolute URI with LRU eviction.
    init_segment_cache: LruCache<String, Bytes>,
    /// Known sizes (in bytes) of media segments keyed by sequence.
    segment_sizes: HashMap<u64, u64>,
}

#[derive(Debug, Clone)]
struct PlaylistSnapshot {
    end_list: bool,
    last_seq: u64,
    target_duration: Option<std::time::Duration>,
    current_segment: Option<MediaSegment>,
}

impl HlsManager {
    /// Creates a new `HlsManager` (no network I/O).
    pub fn new(
        master_url: url::Url,
        config: Arc<HlsSettings>,
        downloader: ResourceDownloader,
        storage_handle: StorageHandle,
        control_sender: mpsc::Sender<StreamMsg>,
    ) -> Self {
        let downloader = downloader;
        let cached_downloader =
            CachedResourceDownloader::new(downloader.clone(), Some(storage_handle));

        Self {
            master_url,
            config,
            downloader,
            cached_downloader,
            control_sender,
            master: None,
            current_variant_index: None,
            current_media_playlist: None,
            media_playlist_url: None,
            next_segment_index: 0,
            init_segment_sent: false,
            key_cache: HashMap::new(),
            init_segment_cache: LruCache::new(NonZeroUsize::new(8).unwrap()),
            segment_sizes: HashMap::new(),
        }
    }

    /// Returns current settings.
    pub fn settings(&self) -> &Arc<HlsSettings> {
        &self.config
    }

    /// Returns the master playlist URL.
    pub fn master_url(&self) -> &url::Url {
        &self.master_url
    }

    /// Returns the effective configuration.
    pub fn config(&self) -> &HlsSettings {
        &self.config
    }

    /// Returns the underlying downloader.
    pub fn downloader(&self) -> &ResourceDownloader {
        &self.downloader
    }

    /// Returns mutable access to the underlying downloader.
    pub fn downloader_mut(&mut self) -> &mut ResourceDownloader {
        &mut self.downloader
    }

    /// Emits `StoreResource` after a network miss (best-effort).
    #[inline]
    fn emit_store_resource(&self, key: stream_download::source::ResourceKey, data: Bytes) {
        trace!(
            "store_resource: persist request key='{}' ({} bytes)",
            key.0,
            data.len()
        );

        // Best-effort: if the channel is full/closed, skip persistence rather than breaking playback.
        match self
            .control_sender
            .try_send(StreamMsg::Control(StreamControl::StoreResource {
                key,
                data,
            })) {
            Ok(()) => {
                trace!("store_resource: enqueued");
            }
            Err(e) => {
                trace!("store_resource: enqueue failed err='{}'", e);
            }
        }
    }

    fn resolve_url(&self, relative_url: &str) -> HlsResult<String> {
        // If caller already passed an absolute URL, keep it as-is.
        if let Ok(u) = url::Url::parse(relative_url) {
            return Ok(u.to_string());
        }

        // URL base selection: base_url override -> current media playlist URL -> master URL.
        let base = if let Some(ref base_url) = self.config.base_url {
            base_url.clone()
        } else if let Some(ref media_playlist_url) = self.media_playlist_url {
            url::Url::parse(media_playlist_url).map_err(HlsError::base_url_parse)?
        } else {
            self.master_url.clone()
        };

        base.join(relative_url)
            .map(|u| u.into())
            .map_err(HlsError::url_join)
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

    fn current_codec_info(&self) -> HlsResult<Option<crate::parser::CodecInfo>> {
        Ok(self.current_variant()?.codec.clone())
    }

    fn current_variant_info(&self) -> HlsResult<(VariantId, Option<crate::parser::CodecInfo>)> {
        let variant = self.current_variant()?;
        Ok((variant.id, variant.codec.clone()))
    }

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
    async fn try_emit_init_segment(&mut self) -> HlsResult<Option<crate::manager::SegmentType>> {
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
                let downloaded = self
                    .download_segment(&init_segment.uri, init_segment.key.as_ref(), None)
                    .await?;
                // Record init segment size for seek map
                self.set_segment_size(0, downloaded.len() as u64);
                self.init_segment_cache
                    .put(resolved_uri.clone(), downloaded.clone());
                downloaded
            };

            return Ok(Some(crate::manager::SegmentType::Init(SegmentData {
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
    ) -> HlsResult<crate::manager::SegmentType> {
        let codec_info = self.current_codec_info()?;

        let data = self
            .download_segment(&seg.uri, seg.key.as_ref(), Some(seg.sequence))
            .await?;
        // Record media segment size for seek map
        self.set_segment_size(seg.sequence, data.len() as u64);

        self.next_segment_index += 1;

        Ok(crate::manager::SegmentType::Media(SegmentData {
            data,
            variant_id: seg.variant_id,
            codec_info,
            key: seg.key,
            sequence: seg.sequence,
            duration: seg.duration,
        }))
    }

    /// Returns the cached master playlist (if loaded).
    pub fn master(&self) -> Option<&MasterPlaylist> {
        self.master.as_ref()
    }

    /// Returns the selected variant index within `master.variants` (if selected).
    pub fn current_variant_index(&self) -> Option<usize> {
        self.current_variant_index
    }

    /// Returns the cached media playlist for the selected variant (if loaded).
    pub fn current_media_playlist(&self) -> Option<&MediaPlaylist> {
        self.current_media_playlist.as_ref()
    }

    /// Returns the known size (bytes) for a segment sequence (if recorded).
    pub fn segment_size(&self, sequence: u64) -> Option<u64> {
        self.segment_sizes.get(&sequence).copied()
    }

    /// Records an observed segment size (bytes) for a sequence.
    pub fn set_segment_size(&mut self, sequence: u64, size: u64) {
        self.segment_sizes.insert(sequence, size);
    }

    /// Probes and records content length for a segment URI (best-effort).
    pub async fn probe_and_record_segment_size(
        &mut self,
        sequence: u64,
        uri: &str,
    ) -> HlsResult<Option<u64>> {
        let resolved_url = self.resolve_url(uri)?;
        let size_opt = self.downloader.probe_content_length(&resolved_url).await?;
        if let Some(size) = size_opt {
            self.segment_sizes.insert(sequence, size);
        }
        Ok(size_opt)
    }

    /// Loads and parses the master playlist, caching it in `self.master`.
    pub async fn load_master(&mut self) -> HlsResult<&MasterPlaylist> {
        let master_hash = master_hash_from_url(&self.master_url);
        let key = cache_keys::playlist_key_from_url(&master_hash, self.master_url.as_str())
            .ok_or_else(|| {
                HlsError::Message("unable to derive master playlist basename".to_string())
            })?;

        let res = self
            .cached_downloader
            .download_playlist_cached(self.master_url.as_str(), &key)
            .await?;

        if res.source == CacheSource::Network {
            self.emit_store_resource(key.clone(), res.bytes.clone());
        }

        let master_playlist = parse_master_playlist(&res.bytes)?;
        self.master = Some(master_playlist);
        Ok(self.master.as_ref().unwrap())
    }

    /// Selects a variant by index and loads its media playlist.
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

        let master_hash = master_hash_from_url(&self.master_url);
        let key = cache_keys::playlist_key_from_url(&master_hash, &media_playlist_url).ok_or_else(
            || HlsError::Message("unable to derive media playlist basename".to_string()),
        )?;

        let res = self
            .cached_downloader
            .download_playlist_cached(&media_playlist_url, &key)
            .await?;

        if res.source == CacheSource::Network {
            self.emit_store_resource(key.clone(), res.bytes.clone());
        }

        let media_playlist = parse_media_playlist(&res.bytes, variant.id)?;

        // Preserve playback position by keeping the same segment index (assumes time-aligned variants).
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
        self.init_segment_sent = false;

        Ok(())
    }

    /// Switch to another variant (wrapper around `select_variant`).
    ///
    /// This method is intended to be used by an ABR controller.
    pub async fn switch_variant(&mut self, new_index: usize) -> HlsResult<()> {
        self.select_variant(new_index).await
    }

    /// Refresh the media playlist for the currently selected variant and return the updated playlist.
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
        let master_hash = master_hash_from_url(&self.master_url);
        let key = cache_keys::playlist_key_from_url(&master_hash, &media_url).ok_or_else(|| {
            HlsError::Message("unable to derive media playlist basename".to_string())
        })?;

        let res = self
            .cached_downloader
            .download_playlist_cached(&media_url, &key)
            .await?;

        if res.source == CacheSource::Network {
            self.emit_store_resource(key.clone(), res.bytes.clone());
        }

        let media_playlist = parse_media_playlist(&res.bytes, variant_id)?;
        self.current_media_playlist = Some(media_playlist);

        Ok(self.current_media_playlist.as_ref().unwrap())
    }

    /// Download segment bytes by URI with optional encryption key.
    ///
    /// `sequence` is used for IV derivation for media segments (None for init segments).
    /// When `aes-decrypt` is enabled, AES-128 decryption is applied if the playlist advertises it.
    #[cfg(feature = "aes-decrypt")]
    fn finalize_key_url(&self, abs_key_url: &str) -> HlsResult<String> {
        if let Some(params) = &self.config.key_query_params {
            let mut url = url::Url::parse(abs_key_url)
                .map_err(|e| HlsError::io_kind(std::io::ErrorKind::InvalidInput, e.to_string()))?;
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

    #[cfg(feature = "aes-decrypt")]
    async fn fetch_key_bytes(&mut self, final_key_url: &str) -> HlsResult<Bytes> {
        if let Some(cached) = self.key_cache.get(final_key_url) {
            return Ok(cached.clone());
        }

        // Variant-scoped key caching:
        // key path: `<master_hash>/<variant_id>/<key_basename>`
        let master_hash = master_hash_from_url(&self.master_url);
        let variant_id = self.current_variant()?.id;

        let key = cache_keys::key_key_from_url(&master_hash, variant_id, final_key_url)
            .ok_or_else(|| HlsError::Message("unable to derive key basename".to_string()))?;

        let res = self
            .cached_downloader
            .download_key_cached(final_key_url, &key)
            .await?;

        if res.source == CacheSource::Network {
            self.emit_store_resource(key.clone(), res.bytes.clone());
        }

        let mut kb = res.bytes;
        if let Some(cb) = &self.config.key_processor_cb {
            kb = (cb)(kb);
        }

        if kb.len() != 16 {
            return Err(HlsError::invalid_aes128_key_len(kb.len()));
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

    /// Resolves AES-128-CBC decryption parameters `(key, iv)` for a segment.
    ///
    /// Returns `Ok(Some((key, iv)))` for AES-128 segments and `Ok(None)` when decryption is not applicable.
    #[cfg(feature = "aes-decrypt")]
    pub async fn resolve_aes128_cbc_params(
        &mut self,
        key: Option<&SegmentKey>,
        sequence: Option<u64>,
    ) -> HlsResult<Option<([u8; 16], [u8; 16])>> {
        if let Some(seg_key) = key {
            if matches!(seg_key.method, EncryptionMethod::Aes128) {
                if let Some(ref key_info) = seg_key.key_info {
                    if let Some(key_uri) = &key_info.uri {
                        let abs_key_url = self.resolve_url(key_uri)?;
                        let final_key_url = self.finalize_key_url(&abs_key_url)?;
                        let key_bytes = self.fetch_key_bytes(&final_key_url).await?;
                        if key_bytes.len() != 16 {
                            return Err(HlsError::invalid_aes128_key_len(key_bytes.len()));
                        }
                        let mut key_arr = [0u8; 16];
                        key_arr.copy_from_slice(&key_bytes);
                        let iv = Self::compute_iv(key_info, sequence);
                        return Ok(Some((key_arr, iv)));
                    }
                }
            }
        }
        Ok(None)
    }

    async fn decrypt_segment_if_needed(
        &mut self,
        data: Bytes,
        _key: Option<&SegmentKey>,
        _sequence: Option<u64>,
    ) -> HlsResult<Bytes> {
        // Decryption is performed in the `HlsStreamWorker` pipeline via `StreamMiddleware`
        // (e.g. `Aes128CbcMiddleware`) when `aes-decrypt` is enabled.
        //
        // `HlsManager` may still resolve key/IV parameters for the worker, but it does not
        // transform segment bytes here.
        Ok(data)
    }

    async fn download_segment(
        &mut self,
        uri: &str,
        key: Option<&SegmentKey>,
        sequence: Option<u64>,
    ) -> HlsResult<Bytes> {
        let resolved_url = self.resolve_url(uri)?;
        let data = self.downloader.download_bytes(&resolved_url).await?;
        let data = self.decrypt_segment_if_needed(data, key, sequence).await?;
        Ok(data)
    }
}

#[derive(Debug, Clone)]
pub struct SegmentDescriptor {
    pub uri: String,
    pub sequence: u64,
    pub is_init: bool,
    pub duration: std::time::Duration,
    pub variant_id: VariantId,
    pub codec_info: Option<crate::parser::CodecInfo>,
    pub key: Option<SegmentKey>,
}

#[derive(Debug, Clone)]
pub enum NextSegmentDescResult {
    Segment(SegmentDescriptor),
    EndOfStream,
    NeedsRefresh { wait: std::time::Duration },
}

impl HlsManager {
    /// Non-blocking descriptor-based API mirroring `next_segment_nonblocking` logic.
    /// Returns a segment descriptor suitable for opening a streaming HTTP connection.
    pub async fn next_segment_descriptor_nonblocking(
        &mut self,
    ) -> HlsResult<NextSegmentDescResult> {
        let PlaylistSnapshot {
            end_list,
            last_seq,
            target_duration,
            current_segment: seg_opt,
        } = self.playlist_snapshot()?;

        // 1) Init segment (at most once per variant selection)
        if !self.init_segment_sent {
            if let Some(init_segment) = self
                .current_media_playlist
                .as_ref()
                .and_then(|p| p.init_segment.as_ref())
                .cloned()
            {
                let desc = self.build_init_descriptor(&init_segment)?;
                self.init_segment_sent = true;
                return Ok(NextSegmentDescResult::Segment(desc));
            } else {
                // Mark even if absent to avoid re-checking
                self.init_segment_sent = true;
            }
        }

        // 2) Media segment available at current index
        if let Some(seg) = seg_opt {
            let desc = self.build_media_descriptor(&seg)?;
            self.next_segment_index += 1;
            return Ok(NextSegmentDescResult::Segment(desc));
        }

        // 3) No segment at current index
        if end_list {
            return Ok(NextSegmentDescResult::EndOfStream);
        }

        // 4) LIVE - refresh playlist and check for new segments
        let (found_new_idx, _new_total, new_end_list, interval) =
            self.live_refresh_cycle(last_seq, target_duration).await?;

        if let Some(idx) = found_new_idx {
            self.next_segment_index = idx;

            let seg = self
                .current_media_playlist
                .as_ref()
                .and_then(|pl| pl.segments.get(idx))
                .cloned()
                .ok_or_else(|| {
                    HlsError::Message("segment index out of bounds after refresh".to_string())
                })?;

            let desc = self.build_media_descriptor(&seg)?;
            // Mirror the byte-based flow where the index is advanced after yield
            self.next_segment_index = idx + 1;
            return Ok(NextSegmentDescResult::Segment(desc));
        }

        if new_end_list {
            return Ok(NextSegmentDescResult::EndOfStream);
        }

        Ok(NextSegmentDescResult::NeedsRefresh { wait: interval })
    }

    fn build_init_descriptor(
        &self,
        init_segment: &crate::parser::InitSegment,
    ) -> HlsResult<SegmentDescriptor> {
        let (variant_id, codec_info) = self.current_variant_info()?;
        let resolved_uri = self.resolve_url(&init_segment.uri)?;
        Ok(SegmentDescriptor {
            uri: resolved_uri,
            sequence: 0,
            is_init: true,
            duration: std::time::Duration::from_secs(0),
            variant_id,
            codec_info,
            key: init_segment.key.clone(),
        })
    }

    fn build_media_descriptor(&self, seg: &MediaSegment) -> HlsResult<SegmentDescriptor> {
        let codec_info = self.current_codec_info()?;
        let resolved_uri = self.resolve_url(&seg.uri)?;
        Ok(SegmentDescriptor {
            uri: resolved_uri,
            sequence: seg.sequence,
            is_init: false,
            duration: seg.duration,
            variant_id: seg.variant_id,
            codec_info,
            key: seg.key.clone(),
        })
    }

    /// Helper to get or probe the size of a segment in bytes.
    async fn get_or_probe_segment_size(
        &mut self,
        sequence: u64,
        uri: &str,
    ) -> HlsResult<Option<u64>> {
        if let Some(sz) = self.segment_size(sequence) {
            return Ok(Some(sz));
        }
        let size_opt = self.probe_and_record_segment_size(sequence, uri).await?;
        Ok(size_opt)
    }

    /// Resolves an absolute byte offset into `(segment_descriptor, intra_segment_offset)`.
    ///
    /// Returns an error if the media playlist is not loaded or the offset is outside the currently known window.
    pub async fn resolve_position(
        &mut self,
        byte_offset: u64,
    ) -> HlsResult<(SegmentDescriptor, u64)> {
        // Clone minimal playlist data so we can probe sizes with `&mut self` without borrow conflicts.
        let (init_opt, media_entries): (
            Option<InitSegment>,
            Vec<(u64, String, std::time::Duration, Option<SegmentKey>)>,
        ) = {
            let pl = self.current_media_playlist.as_ref().ok_or_else(|| {
                HlsError::Message("no media playlist loaded; call select_variant first".to_string())
            })?;
            let init = pl.init_segment.clone();
            let entries = pl
                .segments
                .iter()
                .map(|s| (s.sequence, s.uri.clone(), s.duration, s.key.clone()))
                .collect();
            (init, entries)
        };

        let mut remaining = byte_offset;

        // 1) Init segment (if present)
        if let Some(init) = init_opt.as_ref() {
            let resolved_uri = self.resolve_url(&init.uri)?;
            if let Some(init_size) = self.get_or_probe_segment_size(0, &resolved_uri).await? {
                if remaining < init_size {
                    // Inside init
                    let desc = self.build_init_descriptor(init)?;
                    return Ok((desc, remaining));
                }
                // Skip init
                remaining = remaining.saturating_sub(init_size);
            }
        }

        // 2) Walk media segments (using cloned metadata)
        for (seq, uri, duration, key) in media_entries.iter() {
            let resolved_uri = self.resolve_url(uri)?;
            let size = match self.get_or_probe_segment_size(*seq, &resolved_uri).await? {
                Some(sz) => sz,
                None => {
                    return Err(HlsError::segment_size_unknown(*seq));
                }
            };

            if remaining < size {
                // Build descriptor manually to avoid borrowing issues
                let (variant_id, codec_info) = self.current_variant_info()?;
                let desc = SegmentDescriptor {
                    uri: resolved_uri,
                    sequence: *seq,
                    is_init: false,
                    duration: *duration,
                    variant_id,
                    codec_info,
                    key: key.clone(),
                };
                return Ok((desc, remaining));
            } else {
                remaining = remaining.saturating_sub(size);
            }
        }

        // 3) If we got here, the position lies beyond the currently known window.
        Err(HlsError::Message(
            "seek position is beyond current window".to_string(),
        ))
    }
    /// Repositions internal state so `next_segment_descriptor_nonblocking` yields `desc` next.
    pub fn seek_to_descriptor(&mut self, desc: &SegmentDescriptor) -> HlsResult<()> {
        let pl = self.current_media_playlist.as_ref().ok_or_else(|| {
            HlsError::Message("no media playlist loaded; call select_variant first".to_string())
        })?;

        if desc.is_init {
            self.init_segment_sent = false;
            self.next_segment_index = 0;
            return Ok(());
        }

        // Find the media segment index by sequence number.
        let idx = pl
            .segments
            .iter()
            .position(|s| s.sequence == desc.sequence)
            .ok_or_else(|| HlsError::segment_sequence_not_found(desc.sequence))?;

        self.init_segment_sent = true; // do not emit init again
        self.next_segment_index = idx;
        Ok(())
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
    async fn next_segment(&mut self) -> HlsResult<Option<crate::manager::SegmentType>> {
        // Blocking version: loop until we get a segment or end of stream
        loop {
            match self.next_segment_nonblocking().await? {
                NextSegmentResult::Segment(seg) => return Ok(Some(seg)),
                NextSegmentResult::EndOfStream => return Ok(None),
                NextSegmentResult::NeedsRefresh { wait } => {
                    tracing::trace!(
                        "HlsManager: LIVE no new segments yet, sleeping for {:?}",
                        wait
                    );
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }

    #[instrument(skip(self), fields(variant_index = ?self.current_variant_index, next_segment_index = self.next_segment_index))]
    async fn next_segment_nonblocking(&mut self) -> HlsResult<NextSegmentResult> {
        // --- Phase 0: ensure we have a playlist selected ---
        let PlaylistSnapshot {
            end_list,
            last_seq,
            target_duration,
            current_segment: seg_opt,
        } = self.playlist_snapshot()?;

        // --- Phase 1: init segment (at most once per variant selection) ---
        if !self.init_segment_sent {
            if let Some(init_seg) = self.try_emit_init_segment().await? {
                // Mark even if absent to avoid re-checking on subsequent calls
                self.init_segment_sent = true;
                return Ok(NextSegmentResult::Segment(init_seg));
            }
        }

        // --- Phase 2: media segment available right now ---
        if let Some(seg) = seg_opt {
            let segment = self.emit_media_segment(seg).await?;
            return Ok(NextSegmentResult::Segment(segment));
        }

        // --- Phase 3: no segment at current index ---
        if end_list {
            tracing::trace!(
                "HlsManager: returning EndOfStream. end_list=true last_seq={} next_segment_index={}",
                last_seq,
                self.next_segment_index
            );
            return Ok(NextSegmentResult::EndOfStream);
        }

        // --- Phase 4: LIVE - refresh playlist and check for new segments ---
        let (found_new_idx, new_total, new_end_list, interval) =
            self.live_refresh_cycle(last_seq, target_duration).await?;

        if let Some(idx) = found_new_idx {
            tracing::trace!(
                "HlsManager: LIVE refresh produced new segment: last_seq={} first_new_idx={}",
                last_seq,
                idx
            );
            self.next_segment_index = idx;

            // Now we have a new segment, fetch it
            let seg = self
                .current_media_playlist
                .as_ref()
                .and_then(|pl| pl.segments.get(idx))
                .cloned()
                .ok_or_else(|| {
                    HlsError::Message("segment index out of bounds after refresh".to_string())
                })?;

            let segment = self.emit_media_segment(seg).await?;
            return Ok(NextSegmentResult::Segment(segment));
        }

        // If the stream ended between refreshes
        if new_end_list {
            tracing::trace!(
                "HlsManager: LIVE refresh indicates end_list=true. last_seq={} total_segments={}",
                last_seq,
                new_total
            );
            return Ok(NextSegmentResult::EndOfStream);
        }

        // No new segments yet, tell caller to wait
        tracing::trace!(
            "HlsManager: LIVE no new segments yet. last_seq={} total_segments={} suggested_wait={:?}",
            last_seq,
            new_total,
            interval
        );

        Ok(NextSegmentResult::NeedsRefresh { wait: interval })
    }
}
