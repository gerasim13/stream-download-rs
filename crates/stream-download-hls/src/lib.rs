//! HLS extension for `stream-download`.
//!
//! This crate is an early PoC intended to explore how HTTP HLS playback
//! can be layered on top of the `stream-download` crate without changing
//! its core abstractions too much.
//!
//! Design goals (for the PoC):
//! - Treat each HLS resource (master playlist, media playlist, segment)
//!   as a separate `stream-download` resource.
//! - Keep this crate focused on HLS-specific concerns (parsing playlists,
//!   tracking segments, basic live updates) while delegating all HTTP and
//!   caching to `stream-download`.
//! - Start simple: single rendition, basic live support, no DRM/keys.
//!
//! This crate будет состоять из нескольких модулей:
//! - `model` — базовые структуры данных (плейлисты, сегменты, ошибки).
//! - `parser` — парсинг master/media плейлистов из M3U8.
//! - `manager` — HlsManager и логика работы с одним HLS-потоком.
//! - `abr` — базовый контроллер адаптивного выбора варианта (ABR).
//! - `downloader` — тонкая обёртка над `stream-download` для скачивания ресурсов.
//!
//! Данный файл (`lib.rs`) играет роль фасада: он реэкспортирует основные
//! типы и функции из внутренних модулей, формируя внешний публичный API
//! крейта `stream-download-hls`.

mod abr;
mod downloader;
mod manager;
mod model;
mod parser;

pub use crate::model::{
    HlsConfig, HlsError, HlsResult, MasterPlaylist, MediaPlaylist, MediaSegment, NewSegment,
    VariantStream,
};

pub use crate::abr::{AbrController, PlaybackMetrics};
pub use crate::downloader::ResourceDownloader;
pub use crate::manager::HlsManager;
pub use crate::model::diff_playlists;
pub use crate::parser::{parse_master_playlist, parse_media_playlist};

// Временный re-export Duration, пока мы активно используем его в публичных типах.
// В дальнейшем может быть удалён, если типы будут инкапсулированы лучше.
pub use std::time::Duration;
