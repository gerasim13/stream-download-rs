//! HLS cache key utilities.
//!
//! This module centralizes the construction of [`ResourceKey`](stream_download::source::ResourceKey)
//! values used for caching **small resources** (playlists and encryption keys) in the persistent
//! on-disk storage.
//!
//! Why this exists
//! ---------------
//! You want **minimal changes** in `HlsManager`: it should not be littered with string formatting
//! details and on-disk layout rules.
//!
//! Instead, all HLS-specific cache key formatting lives here (or in the cached downloader layer),
//! and the manager only passes in the inputs it naturally has:
//! - `master_hash`
//! - `variant_id`
//! - resource URL (or sometimes just the basename if already extracted)
//!
//! Agreed on-disk layout
//! ---------------------
//! - Playlists (master + variant): stored at
//!   `storage_root/<master_hash>/<playlist_basename>`
//!   and addressed by `ResourceKey("<master_hash>/<playlist_basename>")`.
//!
//! - Keys (variant-scoped): stored at
//!   `storage_root/<master_hash>/<variant_id>/<key_basename>`
//!   and addressed by `ResourceKey("<master_hash>/<variant_id>/<key_basename>")`.
//!
//! Notes
//! -----
//! - The basename is taken from the resource URI, with the query (`?`) stripped.
//! - We do **not** synthesize extensions. If the URI has no extension, we keep it that way.
//! - These keys are later mapped to disk paths by a tree-layout storage handle, which sanitizes
//!   components to prevent traversal.
//!
//! This module does **not** perform any IO.
//!
//! Logging
//! -------
//! This module emits `trace` logs when it successfully derives basenames / keys (or fails to),
//! to help debug cache key mapping issues.
//!
//! Deterministic master hash
//! -------------------------
//! The `master_hash_from_url` helper lives here because it is part of the deterministic
//! on-disk layout and cache key strategy used throughout the crate.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use stream_download::source::ResourceKey;

use crate::parser::VariantId;

use tracing::trace;

/// Compute a stable identifier for an HLS "stream" based on the master playlist URL.
///
/// This is used as the `<master_hash>` component in the persistent cache layout:
/// `<cache_root>/<master_hash>/<variant_id>/<segment_basename>`.
///
/// Notes:
/// - This is stable within a given build/toolchain, but not guaranteed to be stable across
///   Rust versions because it relies on the standard library hasher.
/// - If you need cross-version stability, replace this with a fixed hash function (e.g. SHA-256).
pub fn master_hash_from_url(url: &url::Url) -> String {
    let mut hasher = DefaultHasher::new();
    url.as_str().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Best-effort basename extraction for a URI-like string.
///
/// Behavior:
/// - strips query part (anything after `?`)
/// - takes substring after the last `/`
/// - returns `None` if result is empty
///
/// Examples:
/// - `"https://a/b/master.m3u8?token=1"` -> `Some("master.m3u8")`
/// - `"seg-001.ts"` -> `Some("seg-001.ts")`
/// - `"https://a/b/"` -> `None`
pub fn uri_basename_no_query(uri: &str) -> Option<&str> {
    let no_query = uri.split('?').next().unwrap_or(uri);
    let base = no_query.rsplit('/').next().unwrap_or(no_query);
    let out = if base.is_empty() { None } else { Some(base) };

    match out {
        Some(b) => trace!("cache_key: basename derived uri='{}' basename='{}'", uri, b),
        None => trace!("cache_key: basename missing uri='{}'", uri),
    }

    out
}

/// Construct a playlist cache key (master or variant) given a URL string.
///
/// Key format:
/// - `"<master_hash>/<playlist_basename>"`
///
/// Returns `None` if the URL does not have a usable basename.
pub fn playlist_key_from_url(master_hash: &str, playlist_url: &str) -> Option<ResourceKey> {
    let basename = uri_basename_no_query(playlist_url)?;
    let key = playlist_key_from_basename(master_hash, basename);
    trace!(
        "cache_key: playlist key derived master_hash='{}' url='{}' key='{}'",
        master_hash, playlist_url, key.0
    );
    Some(key)
}

/// Construct a playlist cache key (master or variant) given a basename.
///
/// Key format:
/// - `"<master_hash>/<playlist_basename>"`
#[inline]
pub fn playlist_key_from_basename(master_hash: &str, playlist_basename: &str) -> ResourceKey {
    // Keep it allocation-friendly: allocate one String once.
    let key = ResourceKey(format!("{}/{}", master_hash, playlist_basename).into());
    trace!(
        "cache_key: playlist key from basename master_hash='{}' basename='{}' key='{}'",
        master_hash, playlist_basename, key.0
    );
    key
}

/// Construct a variant-scoped key cache key given a URL string.
///
/// Key format:
/// - `"<master_hash>/<variant_id>/<key_basename>"`
///
/// Returns `None` if the URL does not have a usable basename.
pub fn key_key_from_url(
    master_hash: &str,
    variant_id: VariantId,
    key_url: &str,
) -> Option<ResourceKey> {
    let basename = uri_basename_no_query(key_url)?;
    let key = key_key_from_basename(master_hash, variant_id, basename);
    trace!(
        "cache_key: key key derived master_hash='{}' variant_id={} url='{}' key='{}'",
        master_hash, variant_id.0, key_url, key.0
    );
    Some(key)
}

/// Construct a variant-scoped key cache key given a basename.
///
/// Key format:
/// - `"<master_hash>/<variant_id>/<key_basename>"`
#[inline]
pub fn key_key_from_basename(
    master_hash: &str,
    variant_id: VariantId,
    key_basename: &str,
) -> ResourceKey {
    let key = ResourceKey(format!("{}/{}/{}", master_hash, variant_id.0, key_basename).into());
    trace!(
        "cache_key: key key from basename master_hash='{}' variant_id={} basename='{}' key='{}'",
        master_hash, variant_id.0, key_basename, key.0
    );
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uri_basename_no_query() {
        assert_eq!(
            uri_basename_no_query("https://a/b/master.m3u8?token=1"),
            Some("master.m3u8")
        );
        assert_eq!(uri_basename_no_query("seg-001.ts"), Some("seg-001.ts"));
        assert_eq!(uri_basename_no_query("https://a/b/"), None);
        assert_eq!(uri_basename_no_query(""), None);
    }

    #[test]
    fn test_playlist_key_format() {
        let k = playlist_key_from_url("deadbeef", "https://x/y/index.m3u8?z=1").unwrap();
        assert_eq!(&*k.0, "deadbeef/index.m3u8");
    }

    #[test]
    fn test_key_key_format() {
        let k = key_key_from_url("deadbeef", VariantId(3), "https://x/y/key.bin?z=1").unwrap();
        assert_eq!(&*k.0, "deadbeef/3/key.bin");
    }
}
