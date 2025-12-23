use std::collections::HashMap;
use std::io;
use std::io::{Seek, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Path;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
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
    HlsManager, HlsPersistentStorageProvider, HlsSettings, HlsStream, HlsStreamParams,
    HlsStreamWorker, ResourceDownloader,
};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

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
/// - `/seg/{name}` maps to the internal blob key `seg/{name}`
/// - `/{path}` catch-all maps to blob key `{path}`
///
/// Notes:
/// - Optional delay can be injected for v0 media segment responses to simulate poor network.
/// - Variants and their segment ranges are configurable (to simulate "variant starts later").
/// - Request counters are tracked per-path so tests can assert caching behavior (e.g. second run
///   should not re-fetch segments when the cache is warm).
#[derive(Clone)]
pub struct HlsFixture {
    blobs: Arc<HashMap<String, Bytes>>,
    slow_v0_segment_delay: Duration,
    request_counts: Arc<std::sync::Mutex<HashMap<String, u64>>>,
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

impl Default for HlsFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl HlsFixture {
    /// Default fixture layout:
    /// - 2 variants: v0 and v1
    /// - v0 segments: `0..11`
    /// - v1 segments: `12..23`
    /// - v0 media segments are delayed by 250ms each (optional ABR/probing scenarios)
    pub fn new() -> Self {
        Self::with_variant_count(2)
    }

    /// Construct a fixture with `variant_count` variants.
    ///
    /// Segment layout policy (deterministic):
    /// - Each variant `v{i}` gets `SEGMENTS_PER_VARIANT` segments.
    /// - Variant `v{i}` segment indices start at `i * SEGMENTS_PER_VARIANT`.
    ///
    /// This preserves the original 2-variant layout:
    /// - v0: 0..11
    /// - v1: 12..23
    pub fn with_variant_count(variant_count: usize) -> Self {
        assert!(variant_count >= 1, "variant_count must be >= 1");
        const SEGMENTS_PER_VARIANT: usize = 12;
        let slow_v0_segment_delay = Duration::from_millis(250);
        let blobs = Self::build_blobs_with_variants(variant_count, SEGMENTS_PER_VARIANT);
        Self {
            blobs: Arc::new(blobs),
            slow_v0_segment_delay,
            request_counts: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Override delay for `seg/v0_*` responses (use `Duration::ZERO` to disable).
    pub fn with_slow_v0_segment_delay(mut self, d: Duration) -> Self {
        self.slow_v0_segment_delay = d;
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
    pub fn build_storage(
        &self,
        kind: HlsFixtureStorageKind,
        settings: &Settings<HlsStream>,
    ) -> HlsFixtureStorage {
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
        hls_settings: HlsSettings,
        settings: Settings<HlsStream>,
    ) -> (reqwest::Url, StreamDownload<BoxedStorageProvider>) {
        let base_url = self.start().await;
        let reader = self
            .stream_download_boxed_with_base(base_url.clone(), storage_kind, hls_settings, settings)
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
        hls_settings: HlsSettings,
        settings: Settings<HlsStream>,
    ) -> StreamDownload<BoxedStorageProvider> {
        let url = base_url
            .join("master.m3u8")
            .expect("failed to build master url");
        let storage = self.build_storage(storage_kind, &settings);
        let storage_handle = storage.storage_handle();
        let params = HlsStreamParams::new(url, hls_settings, storage_handle);
        match storage {
            HlsFixtureStorage::Persistent { provider, .. } => {
                let provider = BoxedStorageProvider::new(provider);
                StreamDownload::new::<HlsStream>(params, provider, settings).await.expect(
                    "failed to create StreamDownload<HlsStream> for fixture server (persistent)",
                )
            }
            HlsFixtureStorage::Temp { provider, .. } => {
                let provider = BoxedStorageProvider::new(provider);
                StreamDownload::new::<HlsStream>(params, provider, settings)
                    .await
                    .expect("failed to create StreamDownload<HlsStream> for fixture server (temp)")
            }
            HlsFixtureStorage::Memory { provider, .. } => {
                let provider = BoxedStorageProvider::new(provider);
                StreamDownload::new::<HlsStream>(params, provider, settings)
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
        hls_settings: Arc<HlsSettings>,
        data_tx: mpsc::Sender<stream_download::source::StreamMsg>,
    ) -> (reqwest::Url, HlsManager) {
        let base_url = self.start().await;
        let url = base_url
            .join("master.m3u8")
            .expect("failed to build master url");

        let downloader = ResourceDownloader::new(
            hls_settings.request_timeout,
            hls_settings.max_retries,
            hls_settings.retry_base_delay,
            hls_settings.max_retry_delay,
        );

        let manager = HlsManager::new(url, hls_settings, downloader, storage_handle, data_tx);
        (base_url, manager)
    }

    /// Start the fixture server and build an `HlsStreamWorker` wired to it.
    ///
    /// This allows tests to construct a worker explicitly (and later pass it into `HlsStream` via
    /// the new worker-taking constructor), instead of relying on `HlsStream::new_with_config` to
    /// build the worker internally.
    pub async fn worker(
        &self,
        storage_handle: stream_download::storage::StorageHandle,
        hls_settings: Arc<HlsSettings>,
        data_tx: mpsc::Sender<stream_download::source::StreamMsg>,
        seek_rx: mpsc::Receiver<u64>,
        cancel: CancellationToken,
        event_tx: broadcast::Sender<stream_download_hls::StreamEvent>,
        segmented_length: Arc<std::sync::RwLock<stream_download::storage::SegmentedLength>>,
    ) -> (reqwest::Url, HlsStreamWorker) {
        let base_url = self.start().await;
        let url = base_url
            .join("master.m3u8")
            .expect("failed to build master url");

        // Build an injected manager (so tests can "matryoshka" components:
        // server -> manager -> worker -> stream -> StreamDownload).
        let downloader = ResourceDownloader::new(
            hls_settings.request_timeout,
            hls_settings.max_retries,
            hls_settings.retry_base_delay,
            hls_settings.max_retry_delay,
        );
        let manager = HlsManager::new(
            url,
            hls_settings,
            downloader,
            storage_handle.clone(),
            data_tx.clone(),
        );

        let master_hash = stream_download_hls::master_hash_from_url(&base_url);

        let worker = HlsStreamWorker::new_with_manager(
            manager,
            storage_handle,
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
        hls_settings: HlsSettings,
        data_channel_capacity: usize,
        storage_kind: HlsFixtureStorageKind,
        f: F,
    ) -> T
    where
        F: FnOnce(
            mpsc::Receiver<StreamMsg>,
        ) -> Pin<Box<dyn std::future::Future<Output = T> + Send>>,
        T: Send + 'static,
    {
        let (data_tx, data_rx) = mpsc::channel::<StreamMsg>(data_channel_capacity);
        let (_seek_tx, seek_rx) = mpsc::channel::<u64>(16);
        let cancel = CancellationToken::new();
        let (event_tx, _event_rx) = broadcast::channel(16);

        let storage_bundle = self.build_storage(storage_kind, &Settings::default());
        let storage_handle = storage_bundle.storage_handle();

        let (_base_url, worker) = self
            .worker(
                storage_handle,
                Arc::new(hls_settings),
                data_tx,
                seek_rx,
                cancel.clone(),
                event_tx.clone(),
                Arc::new(std::sync::RwLock::new(SegmentedLength::default())),
            )
            .await;

        let worker_task = tokio::spawn(async move { worker.run().await });

        let out = f(data_rx).await;

        cancel.cancel();
        let join = tokio::time::timeout(Duration::from_secs(2), worker_task)
            .await
            .expect("worker task did not shut down in time");

        let worker_result = join.expect("worker task panicked");
        match worker_result {
            Ok(()) => {}
            Err(stream_download_hls::HlsStreamError::Cancelled) => {
                // Expected when we stop the worker after collecting data.
            }
            Err(e) => panic!("worker exited with error: {:?}", e),
        }

        out
    }

    fn build_router(&self) -> Router {
        let blobs = self.blobs.clone();
        let slow_v0_segment_delay = self.slow_v0_segment_delay;
        let request_counts = self.request_counts.clone();

        async fn serve_blob(
            Path(path): Path<String>,
            blobs: Arc<HashMap<String, Bytes>>,
            slow_v0_segment_delay: Duration,
            request_counts: Arc<std::sync::Mutex<HashMap<String, u64>>>,
        ) -> impl IntoResponse {
            // `path` is already without a leading slash due to router patterns.
            let key = path.trim_start_matches('/');
            let req_path = format!("/{}", key);

            // Count every request (including 404s) so tests can assert "was this fetched".
            if let Ok(mut lock) = request_counts.lock() {
                *lock.entry(req_path).or_insert(0) += 1;
            }

            // Optional artificial latency for v0 media segments.
            if slow_v0_segment_delay != Duration::ZERO && key.starts_with("seg/v0_") {
                tokio::time::sleep(slow_v0_segment_delay).await;
            }

            let Some(bytes) = blobs.get(key) else {
                return (StatusCode::NOT_FOUND, HeaderMap::new(), Bytes::new());
            };

            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static(if key.ends_with(".m3u8") {
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

        // One compact router:
        // - "/seg/{name}" maps to "seg/<name>"
        // - "/{path}" serves everything else (master.m3u8, v*.m3u8, init*.bin, etc)
        Router::new()
            .route(
                "/seg/{name}",
                get({
                    let blobs = blobs.clone();
                    let request_counts = request_counts.clone();
                    move |Path(name): Path<String>| {
                        let key = format!("seg/{}", name);
                        serve_blob(
                            Path(key),
                            blobs.clone(),
                            slow_v0_segment_delay,
                            request_counts.clone(),
                        )
                    }
                }),
            )
            .route(
                "/{path}",
                get({
                    let blobs = blobs.clone();
                    let request_counts = request_counts.clone();
                    move |Path(path): Path<String>| {
                        serve_blob(
                            Path(path),
                            blobs.clone(),
                            slow_v0_segment_delay,
                            request_counts.clone(),
                        )
                    }
                }),
            )
    }

    fn build_blobs_with_variants(
        variant_count: usize,
        segments_per_variant: usize,
    ) -> HashMap<String, Bytes> {
        let mut blobs: HashMap<String, Bytes> = HashMap::new();
        Self::put_master(&mut blobs, variant_count);
        for v in 0..variant_count {
            let seg_start = v * segments_per_variant;
            let seg_end = seg_start + segments_per_variant;
            Self::put_variant_playlist_and_payloads(&mut blobs, v, seg_start..seg_end);
        }
        blobs
    }

    fn put_master(blobs: &mut HashMap<String, Bytes>, variant_count: usize) {
        let mut out = String::new();
        out.push_str("#EXTM3U\n");
        out.push_str("#EXT-X-VERSION:7\n\n");
        for v in 0..variant_count {
            // Keep the original 2-variant bandwidths for v0/v1, and scale beyond that.
            let bw = match v {
                0 => 128_000,
                1 => 2_560_000,
                _ => 2_560_000 + (v as u64 * 640_000),
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
    ) {
        fn put_text(blobs: &mut HashMap<String, Bytes>, path: String, body: String) {
            blobs.insert(path, Bytes::from(body));
        }
        fn put_static(blobs: &mut HashMap<String, Bytes>, path: String, body: Bytes) {
            blobs.insert(path, body);
        }
        fn put_segment_text(blobs: &mut HashMap<String, Bytes>, path: String, payload: String) {
            blobs.insert(path, Bytes::from(payload));
        }
        fn build_media_playlist(
            version: u32,
            init_uri: &str,
            seg_prefix: &str,
            range: std::ops::Range<usize>,
        ) -> String {
            let mut out = String::new();
            out.push_str("#EXTM3U\n");
            out.push_str(&format!("#EXT-X-VERSION:{version}\n"));
            out.push_str("#EXT-X-TARGETDURATION:1\n");
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
        let playlist = build_media_playlist(7, &init_path, &seg_prefix, seg_range.clone());
        put_text(blobs, playlist_path, playlist);

        // Init payloads: preserve the "INIT-V0"/"INIT-V1" pattern and extend naturally.
        let init_payload = Bytes::from(format!("INIT-V{v}"));
        put_static(blobs, init_path, init_payload);

        // Media payloads (unique per variant/sequence).
        for i in seg_range {
            put_segment_text(blobs, format!("seg/v{v}_{i}.bin"), format!("V{v}-SEG-{i}"));
        }
    }
}
