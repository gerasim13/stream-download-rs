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
//! - init segments: `"<master_hash>/<variant_id>/init_<basename>"`
//! - media segments: `"<master_hash>/<variant_id>/seg_<basename>"`

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use stream_download::source::ResourceKey;
use tracing::trace;

use crate::downloader::Resource;
use crate::parser::VariantId;

/// Computes a deterministic identifier for a stream from the master playlist URL.
///
/// Note: uses the standard library hasher, so the result is not guaranteed to be stable across Rust versions.
pub fn master_hash_from_url(url: &url::Url) -> String {
    let mut hasher = DefaultHasher::new();
    url.as_str().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Generator for cache keys based on master URL and resource type.
#[derive(Debug, Clone)]
pub struct CacheKeyGenerator {
    master_hash: String,
}

impl CacheKeyGenerator {
    /// Create a new CacheKeyGenerator from a master URL.
    pub fn new(master_url: &url::Url) -> Self {
        Self {
            master_hash: master_hash_from_url(master_url),
        }
    }

    /// Get the master hash.
    pub fn master_hash(&self) -> &str {
        &self.master_hash
    }

    /// Extracts the basename from a URI-like string, ignoring the query string.
    ///
    /// Returns `None` if no basename can be derived.
    fn uri_basename_no_query(uri: &str) -> Option<&str> {
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
    pub fn playlist_key_from_url(&self, playlist_url: &str) -> Option<ResourceKey> {
        let basename = Self::uri_basename_no_query(playlist_url)?;
        let key = self.playlist_key_from_basename(basename);
        trace!(
            "cache_key: playlist key derived master_hash='{}' url='{}' key='{}'",
            self.master_hash, playlist_url, key.0
        );
        Some(key)
    }

    /// Constructs a playlist cache key from a basename: `"<master_hash>/<playlist_basename>"`.
    #[inline]
    pub fn playlist_key_from_basename(&self, playlist_basename: &str) -> ResourceKey {
        // Keep it allocation-friendly: allocate one String once.
        let key = ResourceKey(format!("{}/{}", self.master_hash, playlist_basename).into());
        trace!(
            "cache_key: playlist key from basename master_hash='{}' basename='{}' key='{}'",
            self.master_hash, playlist_basename, key.0
        );
        key
    }

    /// Constructs a variant-scoped key cache key from a URL: `"<master_hash>/<variant_id>/<key_basename>"`.
    ///
    /// Returns `None` if no basename can be derived.
    pub fn key_key_from_url(&self, variant_id: VariantId, key_url: &str) -> Option<ResourceKey> {
        let basename = Self::uri_basename_no_query(key_url)?;
        let key = self.key_key_from_basename(variant_id, basename);
        trace!(
            "cache_key: key key derived master_hash='{}' variant_id={} url='{}' key='{}'",
            self.master_hash, variant_id.0, key_url, key.0
        );
        Some(key)
    }

    /// Constructs a variant-scoped key cache key from a basename: `"<master_hash>/<variant_id>/<key_basename>"`.
    #[inline]
    pub fn key_key_from_basename(&self, variant_id: VariantId, key_basename: &str) -> ResourceKey {
        let key =
            ResourceKey(format!("{}/{}/{}", self.master_hash, variant_id.0, key_basename).into());
        trace!(
            "cache_key: key key from basename master_hash='{}' variant_id={} basename='{}' key='{}'",
            self.master_hash, variant_id.0, key_basename, key.0
        );
        key
    }

    /// Generate a cache key for a resource.
    pub fn generate_key(&self, resource: &Resource) -> Option<ResourceKey> {
        match resource {
            Resource::Master(url) => self.playlist_key_from_url(url.as_str()),
            Resource::MediaPlaylist(url, _variant_id) => {
                // Media playlists use the same format as master playlists
                self.playlist_key_from_url(url.as_str())
            }
            Resource::Key(url, variant_id) => self.key_key_from_url(*variant_id, url.as_str()),
            Resource::InitSegment(url, variant_id) => {
                let basename = Self::uri_basename_no_query(url.as_str())?;
                let key = ResourceKey(
                    format!("{}/{}/init_{}", self.master_hash, variant_id.0, basename).into(),
                );
                trace!(
                    "cache_key: init segment key derived master_hash='{}' variant_id={} url='{}' key='{}'",
                    self.master_hash, variant_id.0, url, key.0
                );
                Some(key)
            }
            Resource::MediaSegment(url, variant_id) => {
                let basename = Self::uri_basename_no_query(url.as_str())?;
                let key = ResourceKey(
                    format!("{}/{}/seg_{}", self.master_hash, variant_id.0, basename).into(),
                );
                trace!(
                    "cache_key: media segment key derived master_hash='{}' variant_id={} url='{}' key='{}'",
                    self.master_hash, variant_id.0, url, key.0
                );
                Some(key)
            }
        }
    }
}

/// Create a cache key callback from a CacheKeyGenerator.
pub fn create_key_callback(
    generator: CacheKeyGenerator,
) -> Arc<dyn Fn(&Resource) -> Option<ResourceKey> + Send + Sync> {
    Arc::new(move |resource| generator.generate_key(resource))
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[test]
    fn test_uri_basename_no_query() {
        let generator = CacheKeyGenerator::new(&Url::parse("https://example.com/").unwrap());

        // Test the private method via public methods
        assert_eq!(
            generator
                .playlist_key_from_url("https://a/b/master.m3u8?token=1")
                .unwrap()
                .0
                .as_ref(),
            format!("{}/master.m3u8", generator.master_hash())
        );

        // Direct test of the private method (we can't call it directly, but we can test through public API)
        let key = generator
            .key_key_from_url(VariantId(0), "seg-001.ts")
            .unwrap();
        assert!(key.0.ends_with("seg-001.ts"));

        // Test edge cases through public API
        assert!(generator.playlist_key_from_url("https://a/b/").is_none());
        assert!(generator.playlist_key_from_url("").is_none());
    }

    #[test]
    fn test_playlist_key_format() {
        let generator = CacheKeyGenerator {
            master_hash: "deadbeef".to_string(),
        };
        let k = generator
            .playlist_key_from_url("https://x/y/index.m3u8?z=1")
            .unwrap();
        assert_eq!(&*k.0, "deadbeef/index.m3u8");
    }

    #[test]
    fn test_key_key_format() {
        let generator = CacheKeyGenerator {
            master_hash: "deadbeef".to_string(),
        };
        let k = generator
            .key_key_from_url(VariantId(3), "https://x/y/key.bin?z=1")
            .unwrap();
        assert_eq!(&*k.0, "deadbeef/3/key.bin");
    }

    #[test]
    fn test_cache_key_generator() {
        let master_url = Url::parse("https://example.com/master.m3u8").unwrap();
        let generator = CacheKeyGenerator::new(&master_url);
        let master_hash = generator.master_hash();

        // Test master playlist
        let master_resource =
            Resource::Master(Url::parse("https://example.com/master.m3u8?token=1").unwrap());
        let key = generator.generate_key(&master_resource).unwrap();
        assert!(key.0.starts_with(&format!("{}/", master_hash)));
        assert!(key.0.ends_with("master.m3u8"));

        // Test media playlist
        let media_resource = Resource::MediaPlaylist(
            Url::parse("https://example.com/variant.m3u8?token=2").unwrap(),
            VariantId(1),
        );
        let key = generator.generate_key(&media_resource).unwrap();
        assert!(key.0.starts_with(&format!("{}/", master_hash)));
        assert!(key.0.ends_with("variant.m3u8"));

        // Test key
        let key_resource = Resource::Key(
            Url::parse("https://example.com/key.bin?token=3").unwrap(),
            VariantId(1),
        );
        let key = generator.generate_key(&key_resource).unwrap();
        assert_eq!(&*key.0, &format!("{}/1/key.bin", master_hash));

        // Test init segment
        let init_resource = Resource::InitSegment(
            Url::parse("https://example.com/init.mp4?token=4").unwrap(),
            VariantId(1),
        );
        let key = generator.generate_key(&init_resource).unwrap();
        assert_eq!(&*key.0, &format!("{}/1/init_init.mp4", master_hash));

        // Test media segment
        let media_seg_resource = Resource::MediaSegment(
            Url::parse("https://example.com/seg-001.ts?token=5").unwrap(),
            VariantId(1),
        );
        let key = generator.generate_key(&media_seg_resource).unwrap();
        assert_eq!(&*key.0, &format!("{}/1/seg_seg-001.ts", master_hash));
    }
}
