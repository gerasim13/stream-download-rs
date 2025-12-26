use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::Url;

use stream_download_audio::{AudioSettings, AudioStream, PlayerEvent};

/// Summary of what an audio test observed while driving an `AudioStream`.
#[derive(Debug)]
pub struct AudioObserveResult {
    pub saw_format_changed: bool,
    pub saw_variant_switched: bool,
    pub saw_end_of_stream: bool,
    pub error_message: Option<String>,
    pub last_event: Option<PlayerEvent>,
    pub total_samples: usize,
}

/// Audio-specific local assets server fixture with optional per-file throttling.
///
/// This is intentionally isolated from the shared `fixtures/setup.rs` server so audio tests can run
/// in parallel without relying on global throttling state.
pub struct AudioAssetsServerFixture {
    addr: std::net::SocketAddr,
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
        let delays = std::sync::Arc::new(tokio::sync::RwLock::new(per_file_delay));

        let router = axum::Router::new().fallback(axum::routing::get({
            let delays = delays.clone();
            move |axum::extract::OriginalUri(uri): axum::extract::OriginalUri| {
                let delays = delays.clone();
                async move { throttled_assets_get(uri, delays).await }
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

        Self { addr }
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
}

/// GET handler used by `AudioAssetsServerFixture`.
async fn throttled_assets_get(
    uri: axum::http::Uri,
    delays: std::sync::Arc<tokio::sync::RwLock<HashMap<String, Duration>>>,
) -> impl axum::response::IntoResponse {
    use axum::response::IntoResponse;

    let key = uri.path().trim_start_matches('/').to_string();

    if key.is_empty() || key.contains("..") {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
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
/// - provide a single place to create `AudioStream` and to drive it while collecting observations
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

    /// Construct an `AudioStream` for the real HLS assets served by an audio-specific server.
    ///
    /// This avoids global throttling shared across tests.
    pub async fn audio_stream_hls_real_assets(
        _server_addr: std::net::SocketAddr,
        audio_settings: AudioSettings,
        hls_settings: stream_download_hls::HlsSettings,
        storage_root: Option<std::path::PathBuf>,
        per_file_delay: Option<HashMap<String, Duration>>,
    ) -> AudioStream<stream_download::storage::temp::TempStorageProvider> {
        let per_file_delay = per_file_delay.unwrap_or_default();

        // Start an isolated server per test invocation (analogous to the HLS fixture server).
        let server = AudioAssetsServerFixture::start(per_file_delay).await;
        let url = server.hls_master_url();

        AudioStream::<stream_download::storage::temp::TempStorageProvider>::new_hls(
            url,
            storage_root,
            audio_settings,
            hls_settings,
        )
        .await
    }

    /// Drain events and PCM for up to `timeout`, returning what was observed.
    ///
    /// This helper is intentionally "black-box": tests should not have to re-implement
    /// the same event+PCM polling loops. Assertions should remain in the test code.
    pub async fn drive_and_observe(
        stream: &mut AudioStream<stream_download::storage::temp::TempStorageProvider>,
        timeout: Duration,
        min_samples: usize,
    ) -> AudioObserveResult {
        let deadline = Instant::now() + timeout;

        let events = stream.subscribe_events();

        let mut saw_format_changed = false;
        let mut saw_variant_switched = false;
        let mut saw_end = false;
        let mut error_message: Option<String> = None;
        let mut last_event: Option<PlayerEvent> = None;

        let mut pcm = vec![0.0f32; 4096];
        let mut total_samples = 0usize;

        while Instant::now() < deadline {
            // Drain events opportunistically.
            loop {
                match events.try_recv() {
                    Ok(Some(ev)) => {
                        match &ev {
                            PlayerEvent::FormatChanged { .. } => saw_format_changed = true,
                            PlayerEvent::VariantSwitched { .. } => saw_variant_switched = true,
                            PlayerEvent::Error { message } => error_message = Some(message.clone()),
                            PlayerEvent::EndOfStream => saw_end = true,
                            _ => {}
                        }
                        last_event = Some(ev);
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            // Pull PCM.
            let n = stream.pop_chunk(&mut pcm);
            if n > 0 {
                total_samples += n;
                if total_samples >= min_samples {
                    break;
                }
            } else {
                // Let producer/decoder make progress.
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }

        AudioObserveResult {
            saw_format_changed,
            saw_variant_switched,
            saw_end_of_stream: saw_end,
            error_message,
            last_event,
            total_samples,
        }
    }
}
