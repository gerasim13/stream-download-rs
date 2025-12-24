use std::collections::HashMap;
use std::io;
use std::io::{Seek, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Path;
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::get;
use bytes::Bytes;
use stream_download::source::{StreamControl, StreamMsg};
use stream_download::storage::memory::MemoryStorageProvider;
use stream_download::storage::{
    ContentLength, ProvidesStorageHandle, SegmentedLength, StorageProvider, StorageReader,
    StorageWriter,
};
use stream_download::{Settings, StreamDownload};
use stream_download_hls::{
    AbrConfig, AbrController, HlsManager, HlsPersistentStorageProvider, HlsSettings, HlsStream,
    HlsStreamParams, HlsStreamWorker, ResourceDownloader, VariantId, master_hash_from_url,
};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use aes::Aes128;
use aes::cipher::BlockEncryptMut;
use cbc::Encryptor;
use cbc::cipher::{KeyIvInit, block_padding::Pkcs7};

use stream_download_hls::KeyProcessorCallback;

/// Boxed storage provider adapter for tests.
///
/// Motivation
/// ----------
/// `StreamDownload<P>` is generic over the concrete `StorageProvider` `P`. That makes it awkward to
/// parameterize integration tests over multiple storage backends (persistent file-tree, temp,
/// memory) because you can't easily return a single `StreamDownload<...>` type from helper
/// functions.
///
/// This module provides `BoxedStorageProvider`, a type-erasing adapter that turns any
/// `StorageProvider` into a single uniform provider where:
/// - `Reader = Box<dyn StorageReader>`
/// - `Writer = DynStorageWriter` (a newtype around `Box<dyn StorageWriter>`)
///
/// We need the `DynStorageWriter` wrapper because `StorageWriter` is not implemented for
/// `Box<dyn StorageWriter>` automatically, and `StorageProvider::Writer` must implement
/// `StorageWriter`.
pub struct BoxedStorageProvider {
    inner: BoxedStorageProviderInner,
    capacity: Option<usize>,
}

enum BoxedStorageProviderInner {
    Factory(
        Option<
            Box<
                dyn FnOnce(ContentLength) -> io::Result<(Box<dyn StorageReader>, DynStorageWriter)>
                    + Send,
            >,
        >,
    ),
}

/// Newtype wrapper so we can implement `StorageWriter` for a boxed trait object.
pub struct DynStorageWriter(Box<dyn StorageWriter>);

impl DynStorageWriter {
    pub fn new(inner: Box<dyn StorageWriter>) -> Self {
        Self(inner)
    }
}

impl Write for DynStorageWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl Seek for DynStorageWriter {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}

impl StorageWriter for DynStorageWriter {
    fn control(&mut self, msg: StreamControl) -> io::Result<()> {
        self.0.control(msg)
    }

    fn get_available_ranges_for(
        &mut self,
        stream_key: &stream_download::source::ResourceKey,
    ) -> io::Result<Option<(ContentLength, Vec<std::ops::Range<u64>>)>> {
        self.0.get_available_ranges_for(stream_key)
    }
}

impl BoxedStorageProvider {
    /// Wrap any concrete storage provider into a boxed provider.
    pub fn new<P>(provider: P) -> Self
    where
        P: StorageProvider + Send + 'static,
        P::Reader: StorageReader + 'static,
        P::Writer: StorageWriter + 'static,
    {
        let mut provider_opt = Some(provider);
        let capacity = provider_opt.as_ref().and_then(|p| p.max_capacity());

        let factory: Box<
            dyn FnOnce(ContentLength) -> io::Result<(Box<dyn StorageReader>, DynStorageWriter)>
                + Send,
        > = Box::new(move |content_length| {
            let provider = provider_opt
                .take()
                .expect("BoxedStorageProvider factory already consumed");
            let (reader, writer) = provider.into_reader_writer(content_length)?;
            Ok((
                Box::new(reader) as Box<dyn StorageReader>,
                DynStorageWriter::new(Box::new(writer) as Box<dyn StorageWriter>),
            ))
        });

        Self {
            inner: BoxedStorageProviderInner::Factory(Some(factory)),
            capacity,
        }
    }
}

impl StorageProvider for BoxedStorageProvider {
    type Reader = Box<dyn StorageReader>;
    type Writer = DynStorageWriter;

    fn into_reader_writer(
        self,
        content_length: ContentLength,
    ) -> io::Result<(Self::Reader, Self::Writer)> {
        match self.inner {
            BoxedStorageProviderInner::Factory(mut opt) => {
                let f = opt
                    .take()
                    .expect("BoxedStorageProvider factory already consumed");
                f(content_length)
            }
        }
    }

    fn max_capacity(&self) -> Option<usize> {
        self.capacity
    }
}

/// Minimal in-memory HLS fixture server used by integration tests.
///
/// This fixture does NOT attempt to generate valid TS/fMP4. It serves simple byte payloads so
/// tests can validate:
/// - variant discovery (master playlist parsing),
/// - variant switching effects (URIs differ),
/// - streamed bytes change when variant changes,
/// - ordering invariants at the `StreamMsg`/storage boundary layer.
///
/// Paths served under base (all generated in-memory):
/// - `/master.m3u8`
/// - `/v{idx}.m3u8` for each variant (e.g. `/v0.m3u8`, `/v1.m3u8`, ...)
/// - `/init{idx}.bin` for each variant
/// - `/seg/{name}` maps to the internal blob key `seg/{name}` (unless remapped; see `base_url_prefix`)
/// - `/{path}` catch-all maps to blob key `{path}`
///
/// Notes:
/// - Optional delay can be injected for v0 media segment responses to simulate poor network.
/// - Variants and their segment ranges are configurable (to simulate "variant starts later").
/// - Request counters are tracked per-path so tests can assert caching behavior (e.g. second run
///   should not re-fetch segments when the cache is warm).
#[derive(Clone)]
pub struct HlsFixture {
    variant_count: usize,
    segments_per_variant: usize,
    blobs: Arc<HashMap<String, Bytes>>,
    segment_delay: Duration,
    request_counts: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    hls_settings: HlsSettings,
    media_payload_bytes: Option<usize>,

    /// Optional base URL override (full URL, not just a prefix string).
    ///
    /// When set, the fixture will derive a *path prefix* from this URL and serve most resources
    /// ONLY under that prefix, to validate that the code under test resolves URLs via `base_url`.
    ///
    /// Example:
    /// - base_url = `http://127.0.0.1:1234/a/b/`
    /// - derived prefix = `a/b` (everything after host)
    ///
    /// Serving policy when enabled:
    /// - `/master.m3u8` is always served at root (bootstrap never changes)
    /// - `/<prefix>/v0.m3u8`, `/<prefix>/v1.m3u8`, ...
    /// - `/<prefix>/init0.bin`, ...
    /// - `/<prefix>/key0.bin`, ...
    /// - `/<prefix>/<segment_name>` (where `<segment_name>` is what the playlist lists, e.g. `v0_0.bin`)
    /// - default non-prefixed paths (except `/master.m3u8`) intentionally return 404
    ///
    /// This is parameterizable: you can pass different base_url paths to ensure there is no hardcoding.
    base_url_override: Option<reqwest::Url>,

    // Optional AES-128 CBC segment encryption ("DRM") for fixture-generated media segments.
    //
    // When enabled:
    // - the variant playlists will include `#EXT-X-KEY:METHOD=AES-128,URI="key{v}.bin"`
    // - each segment payload under `seg/v{v}_*.bin` is encrypted (PKCS#7 padded)
    // - key bytes are served under `key{v}.bin`
    //
    // Additionally, in this mode the fixture can validate that key requests include:
    // - required query params (key_query_params)
    // - required headers (key_request_headers)
    aes128: Option<HlsFixtureAes128Config>,
}

/// Which storage backend to use for tests.
///
/// Notes:
/// - `Persistent` uses the HLS segmented file-tree provider (the "real" persistent cache layout).
/// - `Temp` uses the same persistent provider but places data under a per-test temp directory.
///   This is still file-based, but not intended to be reused across runs.
/// - `Memory` uses in-memory storage for the main stream bytes, while still using a file-tree
///   `StorageHandle` for HLS resource caching (playlists/keys/resources).
#[derive(Clone, Debug)]
pub enum HlsFixtureStorageKind {
    /// Persistent segmented file-tree storage rooted at the provided directory.
    Persistent { storage_root: std::path::PathBuf },
    /// Temp directory-based storage (still file-tree), created under `std::env::temp_dir()`.
    Temp { subdir: String },
    /// In-memory stream storage (for `StreamDownload`), plus file-tree `StorageHandle` for HLS
    /// resource caching under the provided directory.
    Memory {
        resource_cache_root: std::path::PathBuf,
    },
}

/// Storage bundle returned by the fixture for a chosen backend.
///
/// This exists because HLS uses two distinct storage concepts:
/// - `StreamDownload` needs a `StorageProvider` for the main byte stream.
/// - `HlsManager`/`HlsStreamWorker` need a keyed `StorageHandle` for read-before-fetch caching
///   (playlists/keys/etc).
///
/// We keep this as a simple enum with concrete provider types (no generic `impl Trait` escapes).
pub enum HlsFixtureStorage {
    Persistent {
        provider: HlsPersistentStorageProvider,
        storage_handle: stream_download::storage::StorageHandle,
    },
    Temp {
        provider: HlsPersistentStorageProvider,
        storage_handle: stream_download::storage::StorageHandle,
    },
    Memory {
        provider: MemoryStorageProvider,
        storage_handle: stream_download::storage::StorageHandle,
    },
}

impl HlsFixtureStorage {
    pub fn storage_handle(&self) -> stream_download::storage::StorageHandle {
        match self {
            Self::Persistent { storage_handle, .. } => storage_handle.clone(),
            Self::Temp { storage_handle, .. } => storage_handle.clone(),
            Self::Memory { storage_handle, .. } => storage_handle.clone(),
        }
    }
}

#[derive(Clone)]
struct HlsFixtureAes128Config {
    /// Per-variant 16-byte keys. These are the "raw" keys that the HLS client must ultimately use.
    keys_by_variant: Vec<[u8; 16]>,

    /// If set, the playlist will pin the IV (0..0). If false, omit IV and let the client derive it
    /// from media sequence (HLS spec behavior).
    fixed_zero_iv: bool,

    /// If set, the fixture will require these query params on key fetches (`/key{v}.bin?...`).
    required_key_query_params: Option<HashMap<String, String>>,
    /// If set, the fixture will require these headers on key fetches.
    required_key_request_headers: Option<HashMap<String, String>>,

    /// If true, the fixture will serve "wrapped" key bytes from `/key{v}.bin` and encrypt segments
    /// with the *unwrapped* key. In this mode, a `key_processor_cb` is required client-side to
    /// unwrap the key bytes before decryption.
    ///
    /// Wrapping scheme (test-only):
    /// - served_key[i] = raw_key[i] ^ 0xFF
    wrap_key_bytes: bool,
}

impl HlsFixtureAes128Config {
    fn new_default(variant_count: usize) -> Self {
        Self {
            keys_by_variant: HlsFixture::default_variant_keys(variant_count),
            fixed_zero_iv: false,
            required_key_query_params: None,
            required_key_request_headers: None,
            wrap_key_bytes: false,
        }
    }
}

impl Default for HlsFixture {
    fn default() -> Self {
        Self::new_default()
    }
}

impl HlsFixture {
    pub const VARIANT_COUNT: usize = 2;
    pub const SEGMENTS_PER_VARIANT: usize = 12;
    pub const SEGMENT_DELAY: Duration = Duration::from_millis(250);

    pub fn new(variant_count: usize, segments_per_variant: usize, segment_delay: Duration) -> Self {
        assert!(variant_count >= 1, "variant_count must be >= 1");
        let blobs =
            Self::build_blobs_with_variants(variant_count, segments_per_variant, None, None);
        Self {
            variant_count,
            segments_per_variant,
            blobs: Arc::new(blobs),
            segment_delay,
            request_counts: Arc::new(std::sync::Mutex::new(HashMap::new())),
            hls_settings: HlsSettings::default(),
            media_payload_bytes: None,
            base_url_override: None,
            aes128: None,
        }
    }

    /// Default fixture layout:
    /// - 2 variants: v0 and v1
    /// - v0 segments: `0..11`
    /// - v1 segments: `12..23`
    /// - v0 media segments are delayed by 250ms each (optional ABR/probing scenarios)
    pub fn new_default() -> Self {
        Self::new(
            Self::VARIANT_COUNT,
            Self::SEGMENTS_PER_VARIANT,
            Self::SEGMENT_DELAY,
        )
    }

    /// Construct a fixture with `variant_count` variants.
    ///
    /// Segment layout policy (deterministic):
    /// - Each variant `v{i}` gets `SEGMENTS_PER_VARIANT` segments starting at sequence 0.
    ///   (Segments are still namespaced by variant in paths/payloads.)
    ///
    /// This preserves the original 2-variant layout:
    /// - v0: 0..11
    /// - v1: 0..11
    pub fn with_variant_count(variant_count: usize) -> Self {
        Self::new(
            variant_count,
            Self::SEGMENTS_PER_VARIANT,
            Self::SEGMENT_DELAY,
        )
    }

    /// Override delay for `seg/v0_*` responses (use `Duration::ZERO` to disable).
    pub fn with_segment_delay(mut self, d: Duration) -> Self {
        self.segment_delay = d;
        self
    }

    /// Configure a full `base_url` override that affects ONLY the fixture server routing policy.
    ///
    /// The fixture derives a *path prefix* from this URL (everything after host) and serves most
    /// resources only under that prefix, to validate that the code under test resolves URLs via `base_url`.
    ///
    /// Example:
    /// - base_url = `http://127.0.0.1:1234/a/b/`
    /// - derived prefix = `a/b`
    ///
    /// IMPORTANT:
    /// This does NOT modify the client's `HlsSettings`. Tests should set client `base_url`
    /// explicitly via `with_hls_config(...)` (or a dedicated helper) when needed.
    ///
    /// Pass `None` to disable the routing override (serve resources under default paths).
    pub fn with_base_url_override(mut self, base_url: Option<reqwest::Url>) -> Self {
        self.base_url_override = base_url;
        self
    }

    fn derived_base_prefix(&self) -> Option<String> {
        let url = self.base_url_override.as_ref()?;
        let p = url.path().trim_matches('/');

        if p.is_empty() {
            None
        } else {
            Some(p.to_string())
        }
    }

    /// Override media payload size for generated segments. When set, payloads are padded/truncated
    /// to this size (while keeping the variant/segment prefix intact) to drive ABR heuristics.
    pub fn with_media_payload_bytes(mut self, bytes: usize) -> Self {
        self.media_payload_bytes = Some(bytes);
        let blobs = Self::build_blobs_with_variants(
            self.variant_count,
            self.segments_per_variant,
            self.media_payload_bytes,
            self.aes128.as_ref(),
        );
        self.blobs = Arc::new(blobs);
        self
    }

    fn refresh_blobs_from_config(mut self) -> Self {
        let blobs = Self::build_blobs_with_variants(
            self.variant_count,
            self.segments_per_variant,
            self.media_payload_bytes,
            self.aes128.as_ref(),
        );
        self.blobs = Arc::new(blobs);
        self
    }

    fn aes128_mut_or_insert_default(&mut self) -> &mut HlsFixtureAes128Config {
        if self.aes128.is_none() {
            self.aes128 = Some(HlsFixtureAes128Config::new_default(self.variant_count));
        }
        self.aes128.as_mut().expect("just inserted")
    }

    /// Enable AES-128 DRM with default settings (no wrapping, no requirements, IV derived from sequence).
    pub fn with_aes128_drm(mut self) -> Self {
        self.aes128 = Some(HlsFixtureAes128Config::new_default(self.variant_count));
        self.refresh_blobs_from_config()
    }

    /// Small setter: pin IV to 0..0 (useful for deterministic tests).
    pub fn with_aes128_fixed_zero_iv(mut self, fixed_zero_iv: bool) -> Self {
        self.aes128_mut_or_insert_default().fixed_zero_iv = fixed_zero_iv;
        self.refresh_blobs_from_config()
    }

    /// Small setter: server requires query params for key fetch; also configures client to send them.
    pub fn with_key_required_query_params(
        mut self,
        query_params: Option<HashMap<String, String>>,
    ) -> Self {
        self.aes128_mut_or_insert_default()
            .required_key_query_params = query_params.clone();
        self.hls_settings = self.hls_settings.clone().key_query_params(query_params);
        self.refresh_blobs_from_config()
    }

    /// Small setter: server requires headers for key fetch; also configures client to send them.
    ///
    /// Pass `None` to indicate "no requirements"; in that mode the server will not validate headers,
    /// and the client will also be configured to send none.
    pub fn with_key_required_request_headers(
        mut self,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        self.aes128_mut_or_insert_default()
            .required_key_request_headers = headers.clone();
        self.hls_settings = self.hls_settings.clone().key_request_headers(headers);
        self.refresh_blobs_from_config()
    }

    /// Small setter: enable/disable wrapped key serving.
    ///
    /// When enabled, you typically also want `with_key_processor_cb(Some(cb))` on the client.
    pub fn with_wrapped_keys(mut self, wrap_key_bytes: bool) -> Self {
        // Keep deterministic IV to avoid sequence alignment issues in tests focusing on key processing.
        let cfg = self.aes128_mut_or_insert_default();
        cfg.fixed_zero_iv = true;
        cfg.wrap_key_bytes = wrap_key_bytes;
        self.refresh_blobs_from_config()
    }

    /// Small setter: configure the client-side key processor callback.
    pub fn with_key_processor_cb(mut self, cb: Option<Arc<Box<KeyProcessorCallback>>>) -> Self {
        self.hls_settings = self.hls_settings.clone().key_processor_cb(cb);
        self
    }

    // -------------------------------------------------------------------------
    // Backwards-compatible convenience wrappers (keep existing call-sites working)
    // -------------------------------------------------------------------------

    /// Override the full HLS settings used by this fixture (downloader/ABR/etc).
    pub fn with_hls_config(mut self, hls_settings: HlsSettings) -> Self {
        self.hls_settings = hls_settings;
        self
    }

    /// Mutate only the ABR-related knobs on the fixture settings.
    pub fn with_abr_config(mut self, f: impl FnOnce(&mut HlsSettings)) -> Self {
        f(&mut self.hls_settings);
        self
    }

    /// Return how many times the fixture served a given request path (e.g. "/master.m3u8",
    /// "/v0.m3u8", "/seg/v0_0.bin").
    ///
    /// This is intended for tests to assert caching behavior (e.g. warm cache avoids re-fetching).
    pub fn request_count_for(&self, path: &str) -> Result<u64, String> {
        let p = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };
        let lock = self
            .request_counts
            .lock()
            .map_err(|_| "request_counts mutex poisoned".to_string())?;
        Ok(*lock.get(&p).unwrap_or(&0))
    }

    /// Clear all request counters (useful if a test wants to isolate phases within a single run).
    pub fn reset_request_counts(&self) -> Result<(), String> {
        let mut lock = self
            .request_counts
            .lock()
            .map_err(|_| "request_counts mutex poisoned".to_string())?;
        lock.clear();
        Ok(())
    }

    /// Start the fixture server and return the base URL (ending with `/`).
    ///
    /// Server startup follows the same style as `tests/tests/setup.rs`:
    /// - bind a `std::net::TcpListener` on `127.0.0.1:0`,
    /// - mark it non-blocking,
    /// - hand it off to `tokio::net::TcpListener::from_std`,
    /// - spawn `axum::serve` in the background.
    pub async fn start(&self) -> reqwest::Url {
        let app = self.build_router();

        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("failed to bind local fixture server");
        listener
            .set_nonblocking(true)
            .expect("failed to set nonblocking on fixture listener");
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener)
                .expect("failed to convert fixture listener to tokio listener");
            axum::serve(listener, app).await.unwrap();
        });

        reqwest::Url::parse(&format!("http://{}/", addr)).expect("failed to build base url")
    }

    /// Build a storage bundle (provider + keyed `StorageHandle`) for a given backend kind.
    ///
    /// This is the core "storage matryoshka" hook:
    /// - `StreamDownload` uses the returned provider.
    /// - `HlsManager`/`HlsStreamWorker` use the returned `StorageHandle`.
    pub fn build_storage(&self, kind: HlsFixtureStorageKind) -> HlsFixtureStorage {
        let settings = Settings::<HlsStream>::default();
        match kind {
            HlsFixtureStorageKind::Persistent { storage_root } => {
                let prefetch_bytes = std::num::NonZeroUsize::new(
                    (settings.get_prefetch_bytes().saturating_mul(2)) as usize,
                )
                .expect("prefetch_bytes must be non-zero");
                let max_cached_streams = std::num::NonZeroUsize::new(10).unwrap();

                let provider = HlsPersistentStorageProvider::new_hls_file_tree(
                    storage_root,
                    prefetch_bytes,
                    Some(max_cached_streams),
                );
                let storage_handle = provider
                    .storage_handle()
                    .expect("persistent HLS storage provider must vend a StorageHandle");

                HlsFixtureStorage::Persistent {
                    provider,
                    storage_handle,
                }
            }
            HlsFixtureStorageKind::Temp { subdir } => {
                let storage_root = std::env::temp_dir()
                    .join("stream-download-tests")
                    .join(subdir);

                let prefetch_bytes = std::num::NonZeroUsize::new(
                    (settings.get_prefetch_bytes().saturating_mul(2)) as usize,
                )
                .expect("prefetch_bytes must be non-zero");
                let max_cached_streams = std::num::NonZeroUsize::new(10).unwrap();

                let provider = HlsPersistentStorageProvider::new_hls_file_tree(
                    storage_root,
                    prefetch_bytes,
                    Some(max_cached_streams),
                );
                let storage_handle = provider
                    .storage_handle()
                    .expect("temp HLS storage provider must vend a StorageHandle");

                HlsFixtureStorage::Temp {
                    provider,
                    storage_handle,
                }
            }
            HlsFixtureStorageKind::Memory {
                resource_cache_root,
            } => {
                // Main stream bytes are stored in memory (fast, non-persistent).
                // For keyed HLS resources (playlists/keys), we still need a `StorageHandle`.
                //
                // We reuse the HLS file-tree handle implementation by creating a small-resource
                // provider rooted at `resource_cache_root` and extracting its handle.
                let prefetch_bytes = std::num::NonZeroUsize::new(
                    (settings.get_prefetch_bytes().saturating_mul(2)) as usize,
                )
                .expect("prefetch_bytes must be non-zero");
                let max_cached_streams = std::num::NonZeroUsize::new(10).unwrap();

                let resource_provider = HlsPersistentStorageProvider::new_hls_file_tree(
                    resource_cache_root,
                    prefetch_bytes,
                    Some(max_cached_streams),
                );
                let storage_handle = resource_provider
                    .storage_handle()
                    .expect("resource cache provider must vend a StorageHandle");

                HlsFixtureStorage::Memory {
                    provider: MemoryStorageProvider::default(),
                    storage_handle,
                }
            }
        }
    }

    /// Start the fixture server and construct a `StreamDownload<HlsStream>` wired to it using any
    /// storage backend, returning a single uniform type: `StreamDownload<BoxedStorageProvider>`.
    ///
    /// This is the preferred helper for tests that are parameterized over storage backends.
    pub async fn stream_download_boxed(
        &self,
        storage_kind: HlsFixtureStorageKind,
    ) -> (reqwest::Url, StreamDownload<BoxedStorageProvider>) {
        let base_url = self.start().await;
        let reader = self
            .stream_download_boxed_with_base(base_url.clone(), storage_kind)
            .await;
        (base_url, reader)
    }

    /// Same as [`stream_download_boxed`], but reuse an existing fixture server/base URL.
    ///
    /// This is useful when a test wants multiple runs against the *same* HLS URL to assert cache
    /// reuse (persistent storage warmup).
    pub async fn stream_download_boxed_with_base(
        &self,
        base_url: reqwest::Url,
        storage_kind: HlsFixtureStorageKind,
    ) -> StreamDownload<BoxedStorageProvider> {
        let url = base_url
            .join("master.m3u8")
            .expect("failed to build master url");
        let storage = self.build_storage(storage_kind);
        let storage_handle = storage.storage_handle();
        let params = HlsStreamParams::new(url, self.hls_settings.clone(), storage_handle);
        match storage {
            HlsFixtureStorage::Persistent { provider, .. } => {
                let provider = BoxedStorageProvider::new(provider);
                StreamDownload::new::<HlsStream>(params, provider, Settings::default())
                .await
                .expect(
                    "failed to create StreamDownload<HlsStream> for fixture server (persistent)",
                )
            }
            HlsFixtureStorage::Temp { provider, .. } => {
                let provider = BoxedStorageProvider::new(provider);
                StreamDownload::new::<HlsStream>(params, provider, Settings::default())
                    .await
                    .expect("failed to create StreamDownload<HlsStream> for fixture server (temp)")
            }
            HlsFixtureStorage::Memory { provider, .. } => {
                let provider = BoxedStorageProvider::new(provider);
                StreamDownload::new::<HlsStream>(params, provider, Settings::default())
                    .await
                    .expect(
                        "failed to create StreamDownload<HlsStream> for fixture server (memory)",
                    )
            }
        }
    }

    /// Start the fixture server and build an `HlsManager` wired to the same fixture URLs.
    ///
    /// This is intended for tests that want to drive `HlsManager` directly (master parsing,
    /// variant selection, descriptor generation), while still using the fixture server.
    ///
    /// Notes:
    /// - `storage_handle` controls playlist/key caching inside the manager.
    /// - `data_tx` is used for ordered `StoreResource` emissions (persistence of small resources).
    pub async fn manager(
        &self,
        storage_handle: stream_download::storage::StorageHandle,
        data_tx: mpsc::Sender<stream_download::source::StreamMsg>,
    ) -> (reqwest::Url, HlsManager) {
        let base_url = self.start().await;
        let url = base_url
            .join("master.m3u8")
            .expect("failed to build master url");

        let hls_settings = Arc::new(self.hls_settings.clone());
        let downloader = ResourceDownloader::new(
            hls_settings.request_timeout,
            hls_settings.max_retries,
            hls_settings.retry_base_delay,
            hls_settings.max_retry_delay,
            CancellationToken::new(),
        )
        .with_key_request_headers(hls_settings.key_request_headers.clone());

        let manager = HlsManager::new(url, hls_settings, downloader, storage_handle, data_tx);
        (base_url, manager)
    }

    /// Build an `AbrController<HlsManager>` wired to the fixture server.
    ///
    /// This lets tests exercise ABR logic without spinning up a full worker/stream pipeline.
    /// The controller is initialized (master loaded + initial variant selected) before being
    /// returned.
    pub async fn abr_controller(
        &self,
        storage_kind: HlsFixtureStorageKind,
        initial_variant_index: usize,
        manual_variant_id: Option<VariantId>,
    ) -> (reqwest::Url, AbrController<HlsManager>) {
        let storage = self.build_storage(storage_kind);
        let storage_handle = storage.storage_handle();
        let hls_settings = Arc::new(self.hls_settings.clone());

        let (data_tx, _data_rx) = mpsc::channel::<StreamMsg>(16);
        let (base_url, mut manager) = self.manager(storage_handle, data_tx).await;

        manager
            .load_master()
            .await
            .expect("failed to load master playlist");

        let master = manager
            .master()
            .cloned()
            .expect("master playlist should be loaded for abr_controller");
        assert!(
            initial_variant_index < master.variants.len(),
            "initial variant index {initial_variant_index} is out of bounds for {} variants",
            master.variants.len()
        );
        let initial_bandwidth = master.variants[initial_variant_index]
            .bandwidth
            .unwrap_or(0) as f64;

        let abr_cfg = AbrConfig {
            min_buffer_for_up_switch: hls_settings.abr_min_buffer_for_up_switch,
            down_switch_buffer: hls_settings.abr_down_switch_buffer,
            throughput_safety_factor: hls_settings.abr_throughput_safety_factor,
            up_hysteresis_ratio: hls_settings.abr_up_hysteresis_ratio,
            down_hysteresis_ratio: hls_settings.abr_down_hysteresis_ratio,
            min_switch_interval: hls_settings.abr_min_switch_interval,
        };

        let mut controller = AbrController::new(
            manager,
            abr_cfg,
            manual_variant_id,
            initial_variant_index,
            initial_bandwidth,
        );

        controller
            .init()
            .await
            .expect("failed to initialize abr controller");

        (base_url, controller)
    }

    /// Start the fixture server and build an `HlsStreamWorker` wired to it.
    ///
    /// This allows tests to construct a worker explicitly (and later pass it into `HlsStream` via
    /// the new worker-taking constructor), instead of relying on `HlsStream::new_with_config` to
    /// build the worker internally.
    pub async fn worker(
        &self,
        storage_handle: stream_download::storage::StorageHandle,
        data_tx: mpsc::Sender<stream_download::source::StreamMsg>,
        seek_rx: mpsc::Receiver<u64>,
        cancel: CancellationToken,
        event_tx: broadcast::Sender<stream_download_hls::StreamEvent>,
        segmented_length: Arc<std::sync::RwLock<stream_download::storage::SegmentedLength>>,
    ) -> (reqwest::Url, HlsStreamWorker) {
        let (base_url, manager) = self.manager(storage_handle.clone(), data_tx.clone()).await;
        let master_hash = master_hash_from_url(&base_url);

        let hls_settings = Arc::new(self.hls_settings.clone());
        let abr_cfg = AbrConfig {
            min_buffer_for_up_switch: hls_settings.abr_min_buffer_for_up_switch,
            down_switch_buffer: hls_settings.abr_down_switch_buffer,
            throughput_safety_factor: hls_settings.abr_throughput_safety_factor,
            up_hysteresis_ratio: hls_settings.abr_up_hysteresis_ratio,
            down_hysteresis_ratio: hls_settings.abr_down_hysteresis_ratio,
            min_switch_interval: hls_settings.abr_min_switch_interval,
        };

        let worker = HlsStreamWorker::new_with_manager(
            manager,
            storage_handle,
            abr_cfg,
            data_tx,
            seek_rx,
            cancel,
            event_tx,
            master_hash,
            segmented_length,
        )
        .await
        .expect("failed to create HlsStreamWorker for fixture server");

        (base_url, worker)
    }

    /// Convenience wrapper: spawn a worker, hand its data channel to a collector, then shut it down.
    pub async fn run_worker_collecting<F, T>(
        self,
        data_channel_capacity: usize,
        storage_kind: HlsFixtureStorageKind,
        f: F,
    ) -> T
    where
        F: FnOnce(
            mpsc::Receiver<StreamMsg>,
            broadcast::Receiver<stream_download_hls::StreamEvent>,
        ) -> Pin<Box<dyn std::future::Future<Output = T> + Send>>,
        T: Send + 'static,
    {
        let (data_tx, data_rx) = mpsc::channel::<StreamMsg>(data_channel_capacity);
        let (_seek_tx, seek_rx) = mpsc::channel::<u64>(16);
        let cancel = CancellationToken::new();
        let (event_tx, event_rx) = broadcast::channel(16);

        let storage_bundle = self.build_storage(storage_kind);
        let storage_handle = storage_bundle.storage_handle();

        let (_base_url, worker) = self
            .worker(
                storage_handle,
                data_tx,
                seek_rx,
                cancel.clone(),
                event_tx.clone(),
                Arc::new(std::sync::RwLock::new(SegmentedLength::default())),
            )
            .await;

        let worker_task = tokio::spawn(async move { worker.run().await });

        let out = f(data_rx, event_rx).await;

        cancel.cancel();
        let join = tokio::time::timeout(Duration::from_secs(2), worker_task)
            .await
            .expect("worker task did not shut down in time");

        let worker_result = join.expect("worker task panicked");
        match worker_result {
            Ok(()) => {}
            Err(stream_download_hls::HlsError::Cancelled) => {
                // Expected when we stop the worker after collecting data.
            }
            Err(e) => panic!("worker exited with error: {:?}", e),
        }

        out
    }

    fn build_router(&self) -> Router {
        let blobs = self.blobs.clone();
        let segment_delay = self.segment_delay;
        let request_counts = self.request_counts.clone();

        // Optional validation policy for key fetch requests.
        let key_validation = self.aes128.as_ref().map(|cfg| {
            (
                cfg.required_key_query_params.clone(),
                cfg.required_key_request_headers.clone(),
            )
        });

        async fn serve_blob(
            Path(path): Path<String>,
            uri: Uri,
            headers_in: HeaderMap,
            blobs: Arc<HashMap<String, Bytes>>,
            segment_delay: Duration,
            request_counts: Arc<std::sync::Mutex<HashMap<String, u64>>>,
            key_validation: Option<(
                Option<HashMap<String, String>>,
                Option<HashMap<String, String>>,
            )>,
        ) -> impl IntoResponse {
            // `path` is already without a leading slash due to router patterns.
            let key = path.trim_start_matches('/');

            // IMPORTANT:
            // - never parse query params from `{path}` manually (encoding can differ)
            // - use `Uri::query()` + `form_urlencoded` for robust parsing
            // - count requests by *path only* (so `/key0.bin?token=...` counts as `/key0.bin`)
            let path_only = key.split('?').next().unwrap_or(key);
            let req_path = format!("/{}", path_only);

            // Count every request (including 404s) so tests can assert "was this fetched".
            if let Ok(mut lock) = request_counts.lock() {
                *lock.entry(req_path.clone()).or_insert(0) += 1;
            }

            // Validate key requests if configured.
            if path_only.starts_with("key") && path_only.ends_with(".bin") {
                if let Some((required_qp, required_headers)) = key_validation {
                    // Query params validation (robust, decoded).
                    if let Some(req) = required_qp.as_ref() {
                        let pairs: HashMap<String, String> = uri
                            .query()
                            .and_then(|q| {
                                reqwest::Url::parse(&format!("http://fixture.local/?{q}")).ok()
                            })
                            .map(|u| {
                                u.query_pairs()
                                    .into_owned()
                                    .collect::<HashMap<String, String>>()
                            })
                            .unwrap_or_default();

                        for (k, v) in req {
                            match pairs.get(k) {
                                Some(got) if got == v => {}
                                Some(got) => {
                                    return (
                                        StatusCode::BAD_REQUEST,
                                        HeaderMap::new(),
                                        Bytes::from(format!(
                                            "missing/invalid key query param: {k}={v} (got {got})"
                                        )),
                                    );
                                }
                                None => {
                                    return (
                                        StatusCode::BAD_REQUEST,
                                        HeaderMap::new(),
                                        Bytes::from(format!(
                                            "missing/invalid key query param: {k}={v} (got <missing>)"
                                        )),
                                    );
                                }
                            }
                        }
                    }

                    // Header validation.
                    if let Some(reqh) = required_headers.as_ref() {
                        for (k, v) in reqh {
                            match headers_in.get(k.as_str()).and_then(|hv| hv.to_str().ok()) {
                                Some(got) if got == v => {}
                                Some(got) => {
                                    return (
                                        StatusCode::BAD_REQUEST,
                                        HeaderMap::new(),
                                        Bytes::from(format!(
                                            "missing/invalid key request header: {k}: {v} (got {got})"
                                        )),
                                    );
                                }
                                None => {
                                    return (
                                        StatusCode::BAD_REQUEST,
                                        HeaderMap::new(),
                                        Bytes::from(format!(
                                            "missing/invalid key request header: {k}: {v} (got <missing>)"
                                        )),
                                    );
                                }
                            }
                        }
                    }
                }
            }

            // Optional artificial latency for v0 media segments.
            if segment_delay != Duration::ZERO && path_only.starts_with("seg/v0_") {
                tokio::time::sleep(segment_delay).await;
            }

            let Some(bytes) = blobs.get(path_only) else {
                return (StatusCode::NOT_FOUND, HeaderMap::new(), Bytes::new());
            };

            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static(if path_only.ends_with(".m3u8") {
                    "application/vnd.apple.mpegurl"
                } else {
                    "application/octet-stream"
                }),
            );
            headers.insert(
                axum::http::header::CACHE_CONTROL,
                HeaderValue::from_static("no-cache"),
            );

            (StatusCode::OK, headers, bytes.clone())
        }

        // Router layout:
        //
        // - `/master.m3u8` is always served at root (bootstrap URL never changes).
        // - When `base_url_override` is Some(...), we derive a prefix from the URL path:
        //     base_url = http://host:port/a/b/  => prefix = "a/b"
        //
        //   Then we serve most resources ONLY under that prefix:
        //   - `/<prefix>/v0.m3u8`, `/<prefix>/v1.m3u8`, ...
        //   - `/<prefix>/init0.bin`, ...
        //   - `/<prefix>/key0.bin`, ...
        //   - `/<prefix>/<segment_name>` where `<segment_name>` is what the playlist lists (e.g. `v0_0.bin`)
        //
        //   And all default non-prefixed paths (except `/master.m3u8`) intentionally return 404.
        //
        // IMPORTANT (axum routing constraints):
        // We avoid patterns like `v{idx}.m3u8` because axum only allows one parameter per path segment.
        // Instead we serve prefixed resources via a single `/{prefix}/{name}` handler and map `name`
        // to the correct internal blob key.
        let base_url_prefix = self.derived_base_prefix();
        let mut r = Router::new();

        // Master is always at root.
        r = r.route(
            "/master.m3u8",
            get({
                let blobs = blobs.clone();
                let request_counts = request_counts.clone();
                let key_validation = key_validation.clone();
                move |uri: Uri, headers: HeaderMap| async move {
                    serve_blob(
                        Path("master.m3u8".to_string()),
                        uri,
                        headers,
                        blobs.clone(),
                        segment_delay,
                        request_counts.clone(),
                        key_validation.clone(),
                    )
                    .await
                }
            }),
        );

        if let Some(prefix) = base_url_prefix.as_ref() {
            // Serve prefixed resources under an arbitrary multi-segment prefix (e.g. "a/b/c") without
            // duplicating handler logic.
            //
            // Key idea:
            // - We keep the handlers "local" and reusable.
            // - We mount the same mini-router under a generated set of concrete prefix paths up to N=3.
            //
            // Why mount instead of wildcard?
            // - In practice, the router we use here doesn't support a safe/portable "{*tail}" catch-all across
            //   all versions/configurations, and we also want deterministic behavior.
            //
            // Supported after-prefix shapes:
            // - `/<prefix>/{tail}` (single segment tail) for: v0.m3u8, init0.bin, key0.bin
            // - `/<prefix>/seg/{seg_name}` (two segments tail) for: seg/<name> (playlist-listed segments)
            let prefix = prefix.trim_matches('/').to_string();

            // Build a small router mounted at the prefix root.
            // This router does NOT know about the prefix; it only serves tails.
            let tail_router = {
                // Handler for `/<prefix>/{tail}` where tail is ONE segment.
                let one_seg = Router::new().route(
                    "/{tail}",
                    get({
                        let blobs = blobs.clone();
                        let request_counts = request_counts.clone();
                        let key_validation = key_validation.clone();
                        move |Path(tail): Path<String>, uri: Uri, headers: HeaderMap| async move {
                            let blob_key = if blobs.contains_key(&tail) {
                                tail
                            } else {
                                // If it's not an exact blob key, treat it as a segment name.
                                format!("seg/{}", tail)
                            };

                            serve_blob(
                                Path(blob_key),
                                uri,
                                headers,
                                blobs.clone(),
                                segment_delay,
                                request_counts.clone(),
                                key_validation.clone(),
                            )
                            .await
                            .into_response()
                        }
                    }),
                );

                // Handler for `/<prefix>/seg/{seg_name}` (two segments after prefix).
                let seg_two = Router::new().route(
                    "/seg/{seg_name}",
                    get({
                        let blobs = blobs.clone();
                        let request_counts = request_counts.clone();
                        let key_validation = key_validation.clone();
                        move |Path(seg_name): Path<String>, uri: Uri, headers: HeaderMap| async move {
                            let blob_key = format!("seg/{}", seg_name);
                            serve_blob(
                                Path(blob_key),
                                uri,
                                headers,
                                blobs.clone(),
                                segment_delay,
                                request_counts.clone(),
                                key_validation.clone(),
                            )
                            .await
                            .into_response()
                        }
                    }),
                );

                Router::new().merge(seg_two).merge(one_seg)
            };

            // Mount the same `tail_router` under concrete prefix paths up to depth=3.
            //
            // For example, if prefix="a/b/c", we mount:
            // - "/a"                (depth=1)
            // - "/a/b"              (depth=2)
            // - "/a/b/c"            (depth=3)
            //
            // This allows us to parameterize tests across depths and ensures no hardcoding.
            // Also, it keeps code-paths identical for all depths.
            let comps: Vec<&str> = prefix
                .split('/')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();

            // Helper: join first `d` components into a path like "/a/b".
            let mut paths: Vec<String> = Vec::new();
            for d in 1..=3 {
                if comps.len() >= d {
                    let mut p = String::new();
                    for c in &comps[..d] {
                        p.push('/');
                        p.push_str(c);
                    }
                    paths.push(p);
                }
            }

            // Mount under each supported depth path.
            for p in paths {
                r = r.nest(&p, tail_router.clone());
            }
        }

        // Default segment route. When prefix is enabled, this MUST be a hard 404.
        r = r.route(
            "/seg/{name}",
            get({
                let blobs = blobs.clone();
                let request_counts = request_counts.clone();
                let key_validation = key_validation.clone();
                let base_url_prefix = base_url_prefix.clone();
                move |Path(name): Path<String>, uri: Uri, headers: HeaderMap| async move {
                    if base_url_prefix.is_some() {
                        return (StatusCode::NOT_FOUND, HeaderMap::new(), Bytes::new())
                            .into_response();
                    }
                    let key = format!("seg/{}", name);
                    serve_blob(
                        Path(key),
                        uri,
                        headers,
                        blobs.clone(),
                        segment_delay,
                        request_counts.clone(),
                        key_validation.clone(),
                    )
                    .await
                    .into_response()
                }
            }),
        );

        // Everything else.
        // When prefix is enabled, only allow master at root; all other root resources should 404.
        r.route(
            "/{path}",
            get({
                let blobs = blobs.clone();
                let request_counts = request_counts.clone();
                let key_validation = key_validation.clone();
                let base_url_prefix = base_url_prefix.clone();
                move |Path(path): Path<String>, uri: Uri, headers: HeaderMap| async move {
                    if base_url_prefix.is_some() && path != "master.m3u8" {
                        return (StatusCode::NOT_FOUND, HeaderMap::new(), Bytes::new())
                            .into_response();
                    }
                    serve_blob(
                        Path(path),
                        uri,
                        headers,
                        blobs.clone(),
                        segment_delay,
                        request_counts.clone(),
                        key_validation.clone(),
                    )
                    .await
                    .into_response()
                }
            }),
        )
    }

    fn build_blobs_with_variants(
        variant_count: usize,
        segments_per_variant: usize,
        media_payload_bytes: Option<usize>,
        aes128: Option<&HlsFixtureAes128Config>,
    ) -> HashMap<String, Bytes> {
        let mut blobs: HashMap<String, Bytes> = HashMap::new();
        Self::put_master(&mut blobs, variant_count);
        for v in 0..variant_count {
            let seg_start = 0;
            let seg_end = segments_per_variant;
            Self::put_variant_playlist_and_payloads(
                &mut blobs,
                v,
                seg_start..seg_end,
                media_payload_bytes,
                aes128,
            );
        }
        blobs
    }

    fn put_master(blobs: &mut HashMap<String, Bytes>, variant_count: usize) {
        let mut out = String::new();
        out.push_str("#EXTM3U\n");
        out.push_str("#EXT-X-VERSION:7\n\n");
        for v in 0..variant_count {
            // Keep bandwidths monotonic but modest so ABR tests can trigger switches quickly.
            let bw = match v {
                0 => 128_000,
                1 => 256_000,
                _ => 256_000 + (v as u64 * 128_000),
            };
            out.push_str(&format!("#EXT-X-STREAM-INF:BANDWIDTH={bw}\n"));
            out.push_str(&format!("v{v}.m3u8\n"));
        }
        blobs.insert("master.m3u8".to_string(), Bytes::from(out));
    }

    fn put_variant_playlist_and_payloads(
        blobs: &mut HashMap<String, Bytes>,
        variant_index: usize,
        seg_range: std::ops::Range<usize>,
        media_payload_bytes: Option<usize>,
        aes128: Option<&HlsFixtureAes128Config>,
    ) {
        fn put_text(blobs: &mut HashMap<String, Bytes>, path: String, body: String) {
            blobs.insert(path, Bytes::from(body));
        }
        fn put_static(blobs: &mut HashMap<String, Bytes>, path: String, body: Bytes) {
            blobs.insert(path, body);
        }
        fn put_segment_bytes(blobs: &mut HashMap<String, Bytes>, path: String, payload: Bytes) {
            blobs.insert(path, payload);
        }
        fn build_media_playlist(
            version: u32,
            init_uri: &str,
            seg_prefix: &str,
            range: std::ops::Range<usize>,
            aes128: Option<&HlsFixtureAes128Config>,
            variant_index: usize,
        ) -> String {
            let mut out = String::new();
            out.push_str("#EXTM3U\n");
            out.push_str(&format!("#EXT-X-VERSION:{version}\n"));

            // Stabilize sequence/IV behavior across tests:
            // - Explicitly set MEDIA-SEQUENCE to 0 so that descriptor sequence numbers and
            //   playlist segment indices align deterministically.
            out.push_str("#EXT-X-MEDIA-SEQUENCE:0\n");

            out.push_str("#EXT-X-TARGETDURATION:1\n");

            if let Some(cfg) = aes128 {
                // HLS AES-128: key URI relative to playlist.
                let mut key_line =
                    format!("#EXT-X-KEY:METHOD=AES-128,URI=\"key{variant_index}.bin\"");
                if cfg.fixed_zero_iv {
                    key_line.push_str(",IV=0x00000000000000000000000000000000");
                }
                out.push_str(&key_line);
                out.push('\n');
            }

            out.push_str(&format!("#EXT-X-MAP:URI=\"{init_uri}\"\n\n"));
            for i in range.clone() {
                out.push_str("#EXTINF:1.0,\n");
                out.push_str(&format!("seg/{seg_prefix}{i}.bin\n"));
            }
            out.push_str("#EXT-X-ENDLIST\n");
            out
        }

        let v = variant_index;
        let playlist_path = format!("v{v}.m3u8");
        let init_path = format!("init{v}.bin");
        let seg_prefix = format!("v{v}_");

        let playlist =
            build_media_playlist(7, &init_path, &seg_prefix, seg_range.clone(), aes128, v);
        put_text(blobs, playlist_path, playlist);

        // If AES-128 is enabled, publish key bytes at `key{v}.bin`.
        if let Some(cfg) = aes128 {
            let key_path = format!("key{v}.bin");
            let raw_key = cfg
                .keys_by_variant
                .get(v)
                .copied()
                .unwrap_or_else(|| [0u8; 16]);

            let served_key: [u8; 16] = if cfg.wrap_key_bytes {
                // Test-only wrapping: served_key = raw_key ^ 0xFF
                let mut out = [0u8; 16];
                for (i, b) in raw_key.iter().enumerate() {
                    out[i] = b ^ 0xFF;
                }
                out
            } else {
                raw_key
            };

            put_static(blobs, key_path, Bytes::copy_from_slice(&served_key));
        }

        // Init payloads: preserve the "INIT-V0"/"INIT-V1" pattern and extend naturally.
        //
        // When AES-128 DRM is enabled, encrypt the init segment as well (AES-128-CBC + PKCS#7),
        // matching real-world streams and production behavior.
        let init_plain = format!("INIT-V{v}").into_bytes();

        let init_bytes = if let Some(cfg) = aes128 {
            let raw_key = cfg
                .keys_by_variant
                .get(v)
                .copied()
                .unwrap_or_else(|| [0u8; 16]);

            // IV policy:
            // - if playlist pins IV=0, use all-zero IV
            // - otherwise use sequence-derived IV; for init, use media-sequence (0 in this fixture)
            let iv: [u8; 16] = if cfg.fixed_zero_iv {
                [0u8; 16]
            } else {
                // MEDIA-SEQUENCE is 0 in this fixture playlists.
                [0u8; 16]
            };

            // AES-128-CBC with PKCS#7 padding (in-place API).
            let encryptor = Encryptor::<Aes128>::new((&raw_key).into(), (&iv).into());

            let msg_len = init_plain.len();
            let mut buf = vec![0u8; msg_len + 16];
            buf[..msg_len].copy_from_slice(&init_plain);

            let ct = encryptor
                .encrypt_padded_mut::<Pkcs7>(&mut buf, msg_len)
                .expect("fixture AES-128-CBC init encrypt_padded_mut failed");

            Bytes::copy_from_slice(ct)
        } else {
            Bytes::from(init_plain)
        };

        put_static(blobs, init_path, init_bytes);

        // Media payloads (unique per variant/sequence).
        //
        // Stabilize sequence/IV derivation across tests:
        // - treat the segment *index within the playlist* as the media sequence number
        // - use that same number in:
        //   * the segment URI (via `i`)
        //   * the payload marker (`V{v}-SEG-{seq}`)
        //   * AES IV derivation when IV is not pinned
        //
        // Note: we still store blobs as `seg/v{v}_{idx}.bin` to preserve existing tests.
        for (idx, i) in seg_range.enumerate() {
            let seq = i;

            let base = format!("V{v}-SEG-{seq}");
            let payload_str = if let Some(n) = media_payload_bytes {
                if n <= base.len() {
                    base.clone()
                } else {
                    let reps = (n / base.len()) + 1;
                    let mut repeated = base.repeat(reps);
                    repeated.truncate(n);
                    repeated
                }
            } else {
                base.clone()
            };

            let out_bytes = if let Some(cfg) = aes128 {
                // IMPORTANT:
                // Segments are always encrypted with the RAW key (the one the client must ultimately use).
                // If `wrap_key_bytes` is enabled, the fixture will serve a wrapped key from `/key{v}.bin`,
                // and the client must unwrap it via `key_processor_cb` to recover this raw key.
                let raw_key = cfg
                    .keys_by_variant
                    .get(v)
                    .copied()
                    .unwrap_or_else(|| [0u8; 16]);

                // IV policy:
                // - if playlist pins IV=0, we use all-zero IV here as well
                // - otherwise mimic HLS default: IV derived from media sequence number
                let iv: [u8; 16] = if cfg.fixed_zero_iv {
                    [0u8; 16]
                } else {
                    let mut iv = [0u8; 16];
                    iv[8..].copy_from_slice(&(seq as u64).to_be_bytes());
                    iv
                };

                // AES-128-CBC with PKCS#7 padding.
                //
                // `cipher` crate version used here does not provide `encrypt_padded_vec_mut`,
                // so we pad manually by allocating a buffer with extra capacity and using
                // `encrypt_padded_mut`.
                let encryptor = Encryptor::<Aes128>::new((&raw_key).into(), (&iv).into());

                let msg = payload_str.into_bytes();
                let msg_len = msg.len();

                // Allocate enough space for PKCS#7 padding (up to one full block).
                let mut buf = vec![0u8; msg_len + 16];
                buf[..msg_len].copy_from_slice(&msg);

                let ct = encryptor
                    .encrypt_padded_mut::<Pkcs7>(&mut buf, msg_len)
                    .expect("fixture AES-128-CBC encrypt_padded_mut failed");

                Bytes::copy_from_slice(ct)
            } else {
                Bytes::from(payload_str)
            };

            put_segment_bytes(blobs, format!("seg/v{v}_{idx}.bin"), out_bytes);
        }
    }

    fn default_variant_keys(variant_count: usize) -> Vec<[u8; 16]> {
        // Deterministic per-variant keys for tests (NOT secure, only for fixture usage).
        // Key v: [v, v+1, v+2, ...] truncated to 16 bytes.
        (0..variant_count)
            .map(|v| {
                let mut k = [0u8; 16];
                for (i, b) in k.iter_mut().enumerate() {
                    *b = (v as u8).wrapping_add(i as u8);
                }
                k
            })
            .collect()
    }
}
