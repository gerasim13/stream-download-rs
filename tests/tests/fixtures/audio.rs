use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::Url;

use stream_download_audio::{AudioControl, AudioDecodeOptions, AudioDecodeStream, AudioMsg};
use stream_download_hls::HlsSettings;

#[derive(Debug, Clone)]
pub struct RequestEntry {
    pub seq: usize,
    pub path: String,
}

/// Summary of what an audio test observed while driving an `AudioDecodeStream`.
///
/// This is intentionally **ordered-only**: we do not rely on out-of-band events for deterministic
/// assertions (same philosophy as `stream-download-hls`).
#[derive(Debug)]
pub struct AudioObserveResult {
    /// Observed ordered `AudioControl::FormatChanged`.
    pub saw_format_changed: bool,

    /// Observed ordered decoder initialization / re-initialization reasons.
    ///
    /// This is the strongest signal that the audio layer actually rebuilt the decoder when an
    /// init epoch changed (e.g. manual/ABR variant switch that changes codec).
    pub decoder_initialized_reasons: Vec<stream_download_audio::DecoderLifecycleReason>,

    /// Ordered HLS init boundaries (by variant index).
    pub ordered_hls_init_starts: Vec<usize>,
    pub ordered_hls_init_ends: Vec<usize>,

    /// Ordered HLS segment boundaries (by variant index + best-effort sequence).
    pub ordered_hls_segment_starts: Vec<(usize, u64)>,
    pub ordered_hls_segment_ends: Vec<(usize, u64)>,

    /// Observed ordered `AudioControl::EndOfStream` (or the stream terminated).
    pub saw_end_of_stream: bool,

    /// Any pipeline error captured by the driver loop (best-effort).
    pub error_message: Option<String>,
    /// Total decoded PCM samples observed (interleaved).
    pub total_samples: usize,
}

/// Audio-specific local assets server fixture with optional per-file throttling.
///
/// This is intentionally isolated from the shared `fixtures/setup.rs` server so audio tests can run
/// in parallel without relying on global throttling state.
pub struct AudioAssetsServerFixture {
    addr: std::net::SocketAddr,
    request_log: Arc<tokio::sync::Mutex<Vec<RequestEntry>>>,
    request_seq: Arc<AtomicUsize>,
}

impl AudioAssetsServerFixture {
    /// Assets root directory (relative to the `tests` crate).
    pub const ASSETS_ROOT: &'static str = "../assets";

    /// Stream chunk size used by throttling.
    const THROTTLE_CHUNK_BYTES: usize = 16 * 1024;

    /// Start a new server instance.
    ///
    /// `per_file_delay` keys are paths relative to the assets root, without a leading `/`, e.g.:
    /// - "hls/index-slq-a1.m3u8"
    /// - "hls/init-slq-a1.mp4"
    /// - "hls/segment-1-slq-a1.m4s"
    ///
    /// Values are "delay per chunk" (see `THROTTLE_CHUNK_BYTES`).
    pub async fn start(per_file_delay: HashMap<String, Duration>) -> Self {
        let delays = Arc::new(tokio::sync::RwLock::new(per_file_delay));
        let request_log: Arc<tokio::sync::Mutex<Vec<RequestEntry>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let request_seq: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

        let router = axum::Router::new().fallback(axum::routing::get({
            let delays = delays.clone();
            let request_log = request_log.clone();
            let request_seq = request_seq.clone();
            move |axum::extract::OriginalUri(uri): axum::extract::OriginalUri| {
                let delays = delays.clone();
                let request_log = request_log.clone();
                let request_seq = request_seq.clone();
                async move { throttled_assets_get(uri, delays, request_log, request_seq).await }
            }
        }));

        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind audio assets server");
        listener
            .set_nonblocking(true)
            .expect("set_nonblocking audio assets server");

        let addr = listener.local_addr().expect("audio assets server addr");

        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            axum::serve(listener, router)
                .await
                .expect("serve audio assets server");
        });

        Self {
            addr,
            request_log,
            request_seq,
        }
    }

    pub fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    pub fn base_url(&self) -> Url {
        Url::parse(&format!("http://{}", self.addr)).expect("base_url")
    }

    pub fn url_for(&self, relative_path: &str) -> Url {
        let rel = relative_path.trim_start_matches('/');
        Url::parse(&format!("http://{}/{}", self.addr, rel)).expect("url_for")
    }

    pub fn hls_master_url(&self) -> Url {
        self.url_for("hls/master.m3u8")
    }

    /// Current request sequence counter value (monotonically increasing).
    ///
    /// This is intended to be used as a "watermark" for correlating player events with requests.
    pub fn request_seq(&self) -> usize {
        self.request_seq.load(Ordering::Relaxed)
    }

    /// Snapshot of all requested asset paths (in order).
    ///
    /// Paths are relative to `ASSETS_ROOT` and do not start with `/`,
    /// e.g. `"hls/master.m3u8"`.
    pub async fn request_log_snapshot(&self) -> Vec<RequestEntry> {
        self.request_log.lock().await.clone()
    }

    /// Wait until a specific asset path is observed in the request log or until timeout.
    ///
    /// Returns `true` if the path was observed.
    pub async fn wait_until_requested(&self, path: &str, timeout: Duration) -> bool {
        let needle = path.trim_start_matches('/');

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            {
                let guard = self.request_log.lock().await;
                if guard.iter().any(|e| e.path == needle) {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    /// Wait until *any* of the provided asset paths is observed in the request log or until timeout.
    ///
    /// Returns the matched path (as provided) if observed.
    pub async fn wait_until_any_requested(
        &self,
        paths: &[&str],
        timeout: Duration,
    ) -> Option<String> {
        let needles: Vec<String> = paths
            .iter()
            .map(|p| p.trim_start_matches('/').to_string())
            .collect();

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            {
                let guard = self.request_log.lock().await;
                for needle in &needles {
                    if guard.iter().any(|e| e.path == *needle) {
                        return Some(needle.clone());
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        None
    }
}

/// GET handler used by `AudioAssetsServerFixture`.
async fn throttled_assets_get(
    uri: axum::http::Uri,
    delays: Arc<tokio::sync::RwLock<HashMap<String, Duration>>>,
    request_log: Arc<tokio::sync::Mutex<Vec<RequestEntry>>>,
    request_seq: Arc<AtomicUsize>,
) -> impl axum::response::IntoResponse {
    use axum::response::IntoResponse;

    let key = uri.path().trim_start_matches('/').to_string();

    if key.is_empty() || key.contains("..") {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }

    {
        let seq = request_seq.fetch_add(1, Ordering::Relaxed);
        let mut guard = request_log.lock().await;
        guard.push(RequestEntry {
            seq,
            path: key.clone(),
        });
    }

    let delay = {
        let guard = delays.read().await;
        guard.get(&key).cloned().unwrap_or(Duration::ZERO)
    };

    let path: PathBuf = PathBuf::from(AudioAssetsServerFixture::ASSETS_ROOT).join(&key);

    let file = match tokio::fs::File::open(&path).await {
        Ok(f) => f,
        Err(_) => return axum::http::StatusCode::NOT_FOUND.into_response(),
    };

    let meta = match file.metadata().await {
        Ok(m) => m,
        Err(_) => return axum::http::StatusCode::NOT_FOUND.into_response(),
    };

    let stream = futures_util::stream::unfold((file, false), move |(mut file, done)| async move {
        if done {
            return None;
        }

        let mut buf = vec![0u8; AudioAssetsServerFixture::THROTTLE_CHUNK_BYTES];
        match tokio::io::AsyncReadExt::read(&mut file, &mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                if delay != Duration::ZERO {
                    tokio::time::sleep(delay).await;
                }
                Some((Ok::<Bytes, std::io::Error>(Bytes::from(buf)), (file, false)))
            }
            Err(e) => Some((Err(e), (file, true))),
        }
    });

    let content_type = if key.ends_with(".m3u8") {
        "application/vnd.apple.mpegurl"
    } else if key.ends_with(".mp4") || key.ends_with(".m4s") {
        "video/mp4"
    } else if key.ends_with(".mp3") {
        "audio/mpeg"
    } else {
        "application/octet-stream"
    };

    let mut resp = axum::response::Response::new(axum::body::Body::from_stream(stream));
    *resp.status_mut() = axum::http::StatusCode::OK;
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static(content_type),
    );
    resp.headers_mut().insert(
        axum::http::header::CONTENT_LENGTH,
        axum::http::HeaderValue::from_str(&meta.len().to_string()).unwrap(),
    );
    resp
}

/// Audio test fixture helpers.
///
/// Purpose
/// -------
/// Organize audio integration tests in the same style as the HLS tests:
/// - centralize knowledge about fixture asset paths
/// - provide a single place to build URLs
/// - provide a single place to create `AudioDecodeStream`
/// - provide deterministic helpers to wait for ordered controls / PCM thresholds
pub struct AudioFixture;

impl AudioFixture {
    /// Root directory (under the assets server) containing real HLS audio test data.
    pub const HLS_ASSETS_DIR: &'static str = "hls";

    /// Master playlist filename inside `HLS_ASSETS_DIR`.
    pub const HLS_MASTER: &'static str = "master.m3u8";

    pub fn hls_master_url(server_addr: std::net::SocketAddr) -> Url {
        Url::parse(&format!(
            "http://{}/{}/{}",
            server_addr,
            Self::HLS_ASSETS_DIR,
            Self::HLS_MASTER
        ))
        .expect("failed to build HLS master URL")
    }

    /// Construct an `AudioDecodeStream` for the real HLS assets served by an audio-specific server.
    ///
    /// This avoids global throttling shared across tests.
    ///
    /// Returns both the `AudioDecodeStream` and the `AudioAssetsServerFixture` so tests can make strict
    /// assertions about the request sequence (e.g. that after ABR switch we fetch segments from
    /// the new variant).
    pub async fn audio_stream_hls_real_assets(
        _server_addr: std::net::SocketAddr,
        hls_settings: stream_download_hls::HlsSettings,
        storage_root: Option<std::path::PathBuf>,
        per_file_delay: Option<HashMap<String, Duration>>,
    ) -> (AudioDecodeStream, AudioAssetsServerFixture) {
        let per_file_delay = per_file_delay.unwrap_or_default();

        // Start an isolated server per test invocation (analogous to the HLS fixture server).
        let server = AudioAssetsServerFixture::start(per_file_delay).await;
        let url = server.hls_master_url();

        let stream = AudioDecodeStream::new_hls(
            url,
            hls_settings,
            AudioDecodeOptions::default(),
            storage_root,
        )
        .await
        .expect("failed to create AudioDecodeStream(HLS)");

        (stream, server)
    }

    /// Construct an `AudioDecodeStream` for the real HLS assets served by an audio-specific server,
    /// but with a helper to mutate the provided `HlsSettings` before stream creation.
    ///
    /// This is intended for tests that need to configure ABR (e.g. `abr_initial_variant_index`)
    /// without repeating boilerplate at call sites.
    pub async fn audio_stream_hls_real_assets_with_hls_settings(
        server_addr: std::net::SocketAddr,
        mut hls_settings: HlsSettings,
        mutate_hls_settings: impl FnOnce(&mut HlsSettings),
        storage_root: Option<std::path::PathBuf>,
        per_file_delay: Option<HashMap<String, Duration>>,
    ) -> (
        stream_download_audio::AudioDecodeStream,
        AudioAssetsServerFixture,
    ) {
        let _ = server_addr;
        mutate_hls_settings(&mut hls_settings);

        Self::audio_stream_hls_real_assets(server_addr, hls_settings, storage_root, per_file_delay)
            .await
    }

    /// Drain ordered controls and PCM for up to `timeout`, returning what was observed.
    ///
    /// NOTE:
    /// - This rewrite intentionally ignores out-of-band events and relies on ordered controls only.
    /// - This is the whole point: strict tests must use ordered protocol signals, like `stream-download-hls`.
    pub async fn drive_and_observe(
        stream: &mut stream_download_audio::AudioDecodeStream,
        timeout: Duration,
        min_samples: usize,
        _request_seq_watermark: impl Fn() -> usize,
    ) -> AudioObserveResult {
        let deadline = Instant::now() + timeout;

        let mut saw_format_changed = false;

        let mut decoder_initialized_reasons: Vec<stream_download_audio::DecoderLifecycleReason> =
            Vec::new();

        let mut ordered_hls_init_starts: Vec<usize> = Vec::new();
        let mut ordered_hls_init_ends: Vec<usize> = Vec::new();
        let mut ordered_hls_segment_starts: Vec<(usize, u64)> = Vec::new();
        let mut ordered_hls_segment_ends: Vec<(usize, u64)> = Vec::new();

        let mut total_samples = 0usize;

        let mut saw_end = false;
        let error_message: Option<String> = None;

        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, stream.next_msg()).await {
                Ok(Some(AudioMsg::Pcm(chunk))) => {
                    total_samples += chunk.pcm.len();
                    if total_samples >= min_samples {
                        break;
                    }
                }
                Ok(Some(AudioMsg::Control(AudioControl::DecoderInitialized { reason }))) => {
                    decoder_initialized_reasons.push(reason);
                }
                Ok(Some(AudioMsg::Control(AudioControl::FormatChanged { .. }))) => {
                    saw_format_changed = true;
                }
                Ok(Some(AudioMsg::Control(AudioControl::HlsInitStart { id }))) => {
                    ordered_hls_init_starts.push(id.variant);
                }
                Ok(Some(AudioMsg::Control(AudioControl::HlsInitEnd { id }))) => {
                    ordered_hls_init_ends.push(id.variant);
                }
                Ok(Some(AudioMsg::Control(AudioControl::HlsSegmentStart { id }))) => {
                    ordered_hls_segment_starts.push((id.variant, id.sequence.unwrap_or(0)));
                }
                Ok(Some(AudioMsg::Control(AudioControl::HlsSegmentEnd { id }))) => {
                    ordered_hls_segment_ends.push((id.variant, id.sequence.unwrap_or(0)));
                }
                Ok(Some(AudioMsg::Control(AudioControl::EndOfStream))) => {
                    saw_end = true;
                    break;
                }
                Ok(None) => {
                    // Stream terminated (channel closed). Treat as end-of-stream for the driver.
                    saw_end = true;
                    break;
                }
                Err(_elapsed) => {
                    // Timed out waiting for the next message; return what we observed so far.
                    // IMPORTANT: do NOT treat this as end-of-stream.
                    break;
                }
            }
        }

        AudioObserveResult {
            saw_format_changed,
            decoder_initialized_reasons,
            ordered_hls_init_starts,
            ordered_hls_init_ends,
            ordered_hls_segment_starts,
            ordered_hls_segment_ends,
            saw_end_of_stream: saw_end,
            error_message,
            total_samples,
        }
    }

    /// Wait until an ordered control matching `pred` is observed, or `timeout` elapses.
    ///
    /// Returns the matched control on success.
    pub async fn wait_for_control(
        stream: &mut AudioDecodeStream,
        timeout: Duration,
        mut pred: impl FnMut(&AudioControl) -> bool,
    ) -> Option<AudioControl> {
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, stream.next_msg()).await {
                Ok(Some(AudioMsg::Control(ctrl))) => {
                    if pred(&ctrl) {
                        return Some(ctrl);
                    }
                }
                Ok(Some(AudioMsg::Pcm(_chunk))) => {
                    // Ignore PCM while waiting for a specific control.
                }
                Ok(None) => return None,
                Err(_elapsed) => break,
            }
        }

        None
    }

    /// Wait until at least `min_samples` PCM samples are observed (interleaved) or `timeout` elapses.
    ///
    /// Returns the number of samples observed.
    pub async fn wait_for_pcm_samples(
        stream: &mut AudioDecodeStream,
        timeout: Duration,
        min_samples: usize,
    ) -> usize {
        let deadline = Instant::now() + timeout;
        let mut total_samples = 0usize;

        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, stream.next_msg()).await {
                Ok(Some(AudioMsg::Pcm(chunk))) => {
                    total_samples += chunk.pcm.len();
                    if total_samples >= min_samples {
                        break;
                    }
                }
                Ok(Some(AudioMsg::Control(_))) => {
                    // Ignore controls while waiting for PCM.
                }
                Ok(None) => break,
                Err(_elapsed) => break,
            }
        }

        total_samples
    }
}
