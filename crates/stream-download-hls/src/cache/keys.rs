//! HLS cache key helpers.
//!
//! This module constructs [`ResourceKey`](stream_download::source::ResourceKey) values for caching
//! small resources (playlists and encryption keys).
//!
//! It does not perform I/O; it only derives deterministic keys from:
//! - a master identifier (`master_hash_from_url`),
//! - a `VariantId` (for variant-scoped keys),
//! - and a URL/basename (query string is ignored when extracting basenames).
//!
//! Key formats used by this module:
//! - playlists: `"<master_hash>/<playlist_basename>"`
//! - keys:      `"<master_hash>/<variant_id>/<key_basename>"`

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use stream_download::source::ResourceKey;

use crate::parser::VariantId;

use tracing::trace;

/// Computes a deterministic identifier for a stream from the master playlist URL.
///
/// Note: uses the standard library hasher, so the result is not guaranteed to be stable across Rust versions.
pub fn master_hash_from_url(url: &url::Url) -> String {
    let mut hasher = DefaultHasher::new();
    url.as_str().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Extracts the basename from a URI-like string, ignoring the query string.
///
/// Returns `None` if no basename can be derived.
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

/// Constructs a playlist cache key from a URL: `"<master_hash>/<playlist_basename>"`.
///
/// Returns `None` if no basename can be derived.
pub fn playlist_key_from_url(master_hash: &str, playlist_url: &str) -> Option<ResourceKey> {
    let basename = uri_basename_no_query(playlist_url)?;
    let key = playlist_key_from_basename(master_hash, basename);
    trace!(
        "cache_key: playlist key derived master_hash='{}' url='{}' key='{}'",
        master_hash, playlist_url, key.0
    );
    Some(key)
}

/// Constructs a playlist cache key from a basename: `"<master_hash>/<playlist_basename>"`.
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

/// Constructs a variant-scoped key cache key from a URL: `"<master_hash>/<variant_id>/<key_basename>"`.
///
/// Returns `None` if no basename can be derived.
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

/// Constructs a variant-scoped key cache key from a basename: `"<master_hash>/<variant_id>/<key_basename>"`.
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
