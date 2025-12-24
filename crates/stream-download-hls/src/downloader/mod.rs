//! Downloader utilities for HLS HTTP fetching.
//!
//! This module groups the network downloader and its cached wrapper under a
//! single namespace while keeping the cache helpers in `crate::cache`.

use bytes::Bytes;
use futures_util::stream::BoxStream;

mod basic;
mod cached;

/// A boxed stream of HLS byte chunks produced by the downloader/manager pipeline.
pub type HlsByteStream = BoxStream<'static, Result<Bytes, crate::error::HlsError>>;

pub use basic::ResourceDownloader;
pub use cached::{CacheSource, CachedBytes, CachedResourceDownloader};
