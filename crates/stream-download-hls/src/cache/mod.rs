//! Cache-related helpers for `stream-download-hls`.
//!
//! This module contains filesystem-level primitives used to implement persistent caching policies
//! (like LRU eviction) safely.
//!
//! In particular, we use a simple **lease file** mechanism to mark caches that are actively in use
//! so that eviction does not delete directories for currently playing streams.

pub mod lease;
