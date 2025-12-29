//! HLS stream manager.
//!
//! This module provides the player-facing API (`MediaStream`) plus segment iteration types.
//! Playlist/key caching is handled via `CacheDownloader` decorator and `StreamControl::StoreResource`.
//! For higher-level design notes, see `crates/stream-download-hls/README.md`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use stream_download::source::{ResourceKey, StreamControl, StreamMsg};

use tokio::sync::mpsc;
use tracing::instrument;

use crate::cache::keys::{master_hash_from_url, playlist_key_from_url};
#[cfg(feature = "aes-decrypt")]
use crate::crypto::resolver::AesKeyResolver;
use crate::downloader::HlsByteStream;
use crate::downloader::{Downloader, DownloaderExt};
use crate::error::{HlsError, HlsResult};
use crate::parser::{
    CodecInfo, InitSegment, MasterPlaylist, MediaPlaylist, MediaSegment, SegmentKey, VariantId,
    VariantStream, parse_master_playlist, parse_media_playlist,
};
use crate::settings::HlsSettings;

/// Player-facing interface for iterating HLS segments.
/// Use [`Self::next_segment`] for blocking behavior, or [`Self::next_segment_nonblocking`] for polling.
#[async_trait]
pub trait MediaStream {
    /// Initializes the stream by fetching and parsing the master playlist.
    async fn init(&mut self) -> HlsResult<()>;

    /// Returns a slice of all available variants (renditions) in the stream.
    fn variants(&self) -> &[VariantStream];

    /// Selects the active variant for subsequent segment fetching.
    async fn select_variant(&mut self, variant: VariantId) -> HlsResult<()>;

    /// Returns the next segment descriptor without blocking for live streams.
    ///
    /// For VOD streams, returns `EndOfStream` when all segments have been fetched.
    /// For live streams, returns `NeedsRefresh` when no new segments are available.
    async fn next_segment(&mut self) -> HlsResult<NextSegmentResult<SegmentDescriptor>>;
}

/// Transforms an `HlsByteStream` (object-safe).
pub trait StreamMiddleware: Send + Sync {
    fn apply(&self, input: HlsByteStream) -> HlsByteStream;
}

/// Result of a non-blocking segment fetch attempt.
#[derive(Debug, Clone)]
pub enum NextSegmentResult<T> {
    /// A segment is available and ready to be processed.
    Segment(T),

    /// End of stream (VOD finished or live playlist has `#EXT-X-ENDLIST`).
    EndOfStream,

    /// Live stream has no new segments yet.
    NeedsRefresh {
        /// Suggested wait duration before the next refresh attempt.
        wait: Duration,
    },
}

#[derive(Debug, Clone)]
struct PlaylistSnapshot {
    end_list: bool,
    last_seq: u64,
    target_duration: Option<Duration>,
    current_segment: Option<MediaSegment>,
}

#[derive(Debug, Clone)]
pub struct SegmentDescriptor {
    pub uri: String,
    pub sequence: u64,
    pub is_init: bool,
    pub duration: Duration,
    pub variant_id: VariantId,
    pub codec_info: Option<CodecInfo>,
    pub key: Option<SegmentKey>,
}

/// High-level handle for a single HLS stream (no network I/O until async methods are called).
pub struct HlsManager {
    /// URL of the master playlist.
    master_url: url::Url,
    /// Configuration parameters.
    config: Arc<HlsSettings>,
    /// Downloader used to fetch playlists and segments.
    downloader: Arc<dyn Downloader + Send + Sync>,
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
    /// AES key resolver for handling AES-128-CBC encryption.
    #[cfg(feature = "aes-decrypt")]
    aes_key_resolver: Option<AesKeyResolver>,
    /// Known sizes (in bytes) of media segments keyed by sequence.
    segment_sizes: std::collections::HashMap<u64, u64>,
}

impl std::fmt::Debug for HlsManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("HlsManager");
        debug_struct
            .field("master_url", &self.master_url)
            .field("config", &self.config)
            .field("downloader", &"Arc<dyn Downloader>")
            .field("control_sender", &self.control_sender)
            .field("master", &self.master)
            .field("current_variant_index", &self.current_variant_index)
            .field("current_media_playlist", &self.current_media_playlist)
            .field("media_playlist_url", &self.media_playlist_url)
            .field("next_segment_index", &self.next_segment_index)
            .field("init_segment_sent", &self.init_segment_sent);

        #[cfg(feature = "aes-decrypt")]
        debug_struct.field("aes_key_resolver", &self.aes_key_resolver);

        debug_struct.field("segment_sizes", &self.segment_sizes);
        debug_struct.finish()
    }
}

impl HlsManager {
    /// Creates a new `HlsManager` (no network I/O).
    pub fn new(
        master_url: url::Url,
        config: Arc<HlsSettings>,
        downloader: Arc<dyn Downloader + Send + Sync>,
        control_sender: mpsc::Sender<StreamMsg>,
    ) -> Self {
        #[cfg(feature = "aes-decrypt")]
        let aes_key_resolver = Some(AesKeyResolver::new(
            Arc::clone(&config),
            downloader.clone(),
            config.key_processor_cb.clone(),
        ));

        Self {
            master_url,
            config,
            downloader,
            control_sender,
            #[cfg(feature = "aes-decrypt")]
            aes_key_resolver,
            master: None,
            current_variant_index: None,
            current_media_playlist: None,
            media_playlist_url: None,
            next_segment_index: 0,
            init_segment_sent: false,
            segment_sizes: std::collections::HashMap::new(),
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
    pub fn downloader(&self) -> &Arc<dyn Downloader + Send + Sync> {
        &self.downloader
    }

    /// Emits `StoreResource` after a network miss (best-effort).
    fn emit_store_resource(&self, key: ResourceKey, data: Bytes) -> HlsResult<()> {
        let msg = StreamMsg::Control(StreamControl::StoreResource { key, data });
        self.control_sender
            .try_send(msg)
            .map_err(|e| HlsError::msg(format!("Failed to send control message: {:?}", e)))
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

    fn current_codec_info(&self) -> HlsResult<Option<CodecInfo>> {
        Ok(self.current_variant()?.codec.clone())
    }

    fn current_variant_info(&self) -> HlsResult<(VariantId, Option<CodecInfo>)> {
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
        let key =
            playlist_key_from_url(&master_hash, self.master_url.as_str()).ok_or_else(|| {
                HlsError::Message("unable to derive master playlist basename".to_string())
            })?;

        let bytes = self.downloader.download(self.master_url.as_str()).await?;
        self.emit_store_resource(key.clone(), bytes.clone())?;

        let master_playlist = parse_master_playlist(&bytes)?;
        self.master = Some(master_playlist);
        Ok(self.master.as_ref().unwrap())
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
        let key = playlist_key_from_url(&master_hash, &media_url).ok_or_else(|| {
            HlsError::Message("unable to derive media playlist basename".to_string())
        })?;

        let bytes = self.downloader.download(&media_url).await?;
        self.emit_store_resource(key.clone(), bytes.clone())?;

        let media_playlist = parse_media_playlist(&bytes, variant_id)?;
        self.current_media_playlist = Some(media_playlist);

        Ok(self.current_media_playlist.as_ref().unwrap())
    }

    /// Resolve DRM (AES-128-CBC) params for a descriptor if applicable.
    #[cfg(feature = "aes-decrypt")]
    pub async fn resolve_drm_params_for_desc(
        &mut self,
        desc: &SegmentDescriptor,
    ) -> HlsResult<Option<([u8; 16], [u8; 16])>> {
        let variant_id = self.current_variant()?.id;
        let master_url = self.master_url.as_str().to_string();

        if let Some(resolver) = &mut self.aes_key_resolver {
            return resolver
                .resolve_drm_params_for_desc(&master_url, variant_id, desc)
                .await;
        }
        Ok(None)
    }

    /// Returns the next segment descriptor without blocking for live streams.
    ///
    /// For VOD streams, returns `EndOfStream` when all segments have been fetched.
    /// For live streams, returns `NeedsRefresh` when no new segments are available.
    pub async fn next_segment_descriptor(
        &mut self,
    ) -> HlsResult<NextSegmentResult<SegmentDescriptor>> {
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
                return Ok(NextSegmentResult::Segment(desc));
            } else {
                // Mark even if absent to avoid re-checking
                self.init_segment_sent = true;
            }
        }

        // 2) Media segment available at current index
        if let Some(seg) = seg_opt {
            let desc = self.build_media_descriptor(&seg)?;
            self.next_segment_index += 1;
            return Ok(NextSegmentResult::Segment(desc));
        }

        // 3) No segment at current index
        if end_list {
            return Ok(NextSegmentResult::EndOfStream);
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
            return Ok(NextSegmentResult::Segment(desc));
        }

        if new_end_list {
            return Ok(NextSegmentResult::EndOfStream);
        }

        Ok(NextSegmentResult::NeedsRefresh { wait: interval })
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

    /// Repositions internal state so `next_segment_descriptor` yields `desc` next.
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

    async fn select_variant(&mut self, variant: VariantId) -> HlsResult<()> {
        let playlist = self
            .master
            .as_ref()
            .and_then(|m| m.variants.get(variant.0))
            .ok_or_else(|| {
                HlsError::Message("variant index out of bounds or master not loaded".to_string())
            })?
            .clone(); // Clone to avoid borrowing issues

        let media_playlist_url = self.resolve_url(&playlist.uri)?;
        let master_hash = master_hash_from_url(&self.master_url);
        let key = playlist_key_from_url(&master_hash, &media_playlist_url).ok_or_else(|| {
            HlsError::Message("unable to derive media playlist basename".to_string())
        })?;

        let bytes = self
            .downloader
            .download_playlist(&media_playlist_url)
            .await?;
        self.emit_store_resource(key.clone(), bytes.clone())?;

        let media_playlist = parse_media_playlist(&bytes, playlist.id)?;

        // Preserve playback position by keeping the same segment index (assumes time-aligned variants).
        let old_index = self.next_segment_index;
        let next_segment_index =
            std::cmp::min(self.next_segment_index, media_playlist.segments.len());

        tracing::debug!(
            "HlsManager: switching variant from {:?} to {} (index {:?} -> {}, segments: {})",
            self.current_variant_index,
            variant.0,
            old_index,
            next_segment_index,
            media_playlist.segments.len()
        );

        self.current_media_playlist = Some(media_playlist);
        self.current_variant_index = Some(variant.0);
        self.media_playlist_url = Some(media_playlist_url);
        self.next_segment_index = next_segment_index;
        self.init_segment_sent = false;

        Ok(())
    }

    #[instrument(skip(self), fields(variant_index = ?self.current_variant_index, next_segment_index = self.next_segment_index))]
    async fn next_segment(&mut self) -> HlsResult<NextSegmentResult<SegmentDescriptor>> {
        // Delegate to the existing implementation
        self.next_segment_descriptor().await
    }
}
