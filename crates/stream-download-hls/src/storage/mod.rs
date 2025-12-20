//! Storage utilities for HLS segmented downloads.
//!
//! This module groups:
//! - the segmented storage provider (`segmented`),
//! - filesystem-backed HLS factories (`hls_factory`),
//! - cache policy layer for segment eviction/leases (`cache_layer`),
//! - read-only tree handle for small resources (`tree_handle`).

mod segmented;

pub mod cache_layer;
pub mod hls_factory;
pub mod tree_handle;

pub use segmented::{SegmentStorageFactory, SegmentedStorageProvider};
