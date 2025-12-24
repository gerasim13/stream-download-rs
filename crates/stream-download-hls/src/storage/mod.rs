//! Segmented storage helpers.
//!
//! This module provides the segmented storage provider plus the HLS-specific factories/layers.
//! High-level storage/caching notes live in `crates/stream-download-hls/README.md`.

mod segmented;

pub mod cache_layer;
pub mod hls_factory;
pub mod tree_handle;

pub use segmented::{SegmentStorageFactory, SegmentedStorageProvider};
