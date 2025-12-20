//! Downloader utilities for HLS HTTP fetching.
//!
//! This module groups the network downloader and its cached wrapper under a
//! single namespace while keeping the cache helpers in `crate::cache`.

mod basic;
mod cached;

pub use basic::ResourceDownloader;
pub use cached::{CacheSource, CachedBytes, CachedResourceDownloader};
