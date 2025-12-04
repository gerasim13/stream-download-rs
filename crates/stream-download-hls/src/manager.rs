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

use crate::downloader::ResourceDownloader;
use crate::model::{
    HlsConfig, HlsError, HlsResult, MasterPlaylist, MediaPlaylist, MediaSegment, VariantId,
    VariantStream,
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
        let _ = (&self.downloader, &self.master_url, &self.config, &parse_master_playlist);
        Err(HlsError::Message(
            "HlsManager::load_master is not implemented yet".to_string(),
        ))
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
        if let Some(master) = &self.master {
            if index >= master.variants.len() {
                return Err(HlsError::Message(format!(
                    "variant index {} out of range ({} variants)",
                    index,
                    master.variants.len()
                )));
            }
            // For now just store the index; media playlist handling will be added later.
            self.current_variant_index = Some(index);
            Ok(())
        } else {
            Err(HlsError::Message(
                "master playlist not loaded; call load_master() first".to_string(),
            ))
        }
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
        let _ = (&self.downloader, &self.current_variant_index, &parse_media_playlist);
        Err(HlsError::Message(
            "HlsManager::refresh_media_playlist is not implemented yet".to_string(),
        ))
    }

    /// Download the bytes for a specific segment.
    ///
    /// This will eventually:
    /// - resolve the segment URI relative to the media playlist URL if needed,
    /// - use `ResourceDownloader` to fetch the segment bytes (possibly with
    ///   caching),
    /// - return the raw bytes to the caller for decoding.
    ///
    /// For now this is a stub that always returns an error.
    pub async fn download_segment(&self, segment: &MediaSegment) -> HlsResult<Vec<u8>> {
        let _ = (&self.downloader, segment);
        Err(HlsError::Message(
            "HlsManager::download_segment is not implemented yet".to_string(),
        ))
    }
}

#[async_trait]
impl MediaStream for HlsManager {
    async fn init(&mut self) -> HlsResult<()> {
        todo!("Will be implemented by calling self.load_master()");
    }

    fn variants(&self) -> &[VariantStream] {
        todo!("Will be implemented by returning from self.master()");
    }

    async fn select_variant(&mut self, variant_id: VariantId) -> HlsResult<()> {
        let _ = variant_id; // avoid unused variable warning
        todo!("Will be implemented by calling self.select_variant(variant_id.0)");
    }

    async fn next_segment(&mut self) -> HlsResult<Option<SegmentData>> {
        todo!("Core media consumption loop will be implemented here");
    }
}
