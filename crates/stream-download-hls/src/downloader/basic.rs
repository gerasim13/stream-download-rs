use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::stream::{BoxStream, Stream, StreamExt};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::error::{HlsError, HlsResult};

// Reuse the `stream-download` HTTP layer and its shared reqwest client so behavior stays
// consistent across the workspace (pooling, DNS cache, retries, etc.).
use stream_download::http::HttpStream;
use stream_download::http::reqwest::Client as ReqwestClient;
use stream_download::http::reqwest::Url;
use stream_download::source::{DecodeError, SourceStream, StreamMsg};

/// HTTP resource downloader for HLS downloads and segment streaming.
///
/// Supports cancellation and bounded retries/backoff. `request_timeout` acts like a progress/idle timeout for streaming reads.
#[derive(Debug, Clone)]
pub struct ResourceDownloader {
    // Request / retry configuration.
    request_timeout: Duration,
    max_retries: u32,
    retry_base_delay: Duration,
    max_retry_delay: Duration,

    // Cancellation token used for all network operations performed by this downloader.
    cancel: CancellationToken,

    // Optional headers to attach to KEY fetch requests (AES-128 / DRM-like flows).
    key_request_headers: Option<HashMap<String, String>>,
}

impl ResourceDownloader {
    // Construction / configuration

    /// Creates a new downloader.
    pub fn new(
        request_timeout: Duration,
        max_retries: u32,
        retry_base_delay: Duration,
        max_retry_delay: Duration,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            request_timeout,
            max_retries,
            retry_base_delay,
            max_retry_delay,
            cancel,
            key_request_headers: None,
        }
    }

    /// Sets optional headers used for AES key fetch requests.
    pub fn with_key_request_headers(
        mut self,
        headers: Option<std::collections::HashMap<String, String>>,
    ) -> Self {
        self.key_request_headers = headers;
        self
    }

    /// Returns the cancellation token used by this downloader.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    // Public API: full downloads

    /// Downloads a URL into memory (with retries and cancellation).
    pub async fn download_bytes(&self, url: &str) -> HlsResult<Bytes> {
        self.download_bytes_inner(url).await
    }

    /// Downloads a playlist into memory.
    pub async fn download_playlist(&self, url: &str) -> HlsResult<Bytes> {
        self.download_bytes(url).await
    }

    /// Downloads an encryption key into memory.
    pub async fn download_key(&self, url: &str) -> HlsResult<Bytes> {
        self.download_key_inner(url).await
    }

    // Public API: streaming segments

    /// Streams a media segment.
    pub async fn stream_segment(
        &self,
        url: &str,
    ) -> HlsResult<BoxStream<'static, Result<Bytes, HlsError>>> {
        let url = Self::parse_url(url)?;
        let url_str = url.to_string();
        let http = self.create_stream(url).await?;
        Ok(Self::map_stream_errors(url_str, http).boxed())
    }

    /// Streams a media segment over a byte range (`start..end`, where `end` is exclusive).
    ///
    /// Pass `None` for an open-ended range.
    pub async fn stream_segment_range(
        &self,
        url: &str,
        start: u64,
        end: Option<u64>,
    ) -> HlsResult<BoxStream<'static, Result<Bytes, HlsError>>> {
        let url = Self::parse_url(url)?;
        let url_str = url.to_string();
        let mut http = self.create_stream(url).await?;
        // `HttpStream` expects an exclusive end value; this matches our API.
        http.seek_range(start, end)
            .await
            .map_err(|e| HlsError::Io(e))?;
        Ok(Self::map_stream_errors(url_str, http).boxed())
    }

    // ----------------------------
    // Internals: download orchestration
    // ----------------------------

    async fn download_bytes_inner(&self, url: &str) -> HlsResult<Bytes> {
        self.retry_with_backoff(url, "download", || self.try_download_once(url))
            .await
    }

    async fn download_key_inner(&self, url: &str) -> HlsResult<Bytes> {
        // Fast path: no custom headers => reuse the existing bytes downloader.
        if self.key_request_headers.is_none() {
            return self.download_bytes_inner(url).await;
        }

        self.retry_with_backoff(url, "key download", || self.try_download_key_once(url))
            .await
    }

    async fn download_via_stream(
        &self,
        url_str: &str,
        http: HttpStream<ReqwestClient>,
    ) -> HlsResult<Bytes> {
        // Timeouts:
        // - `request_timeout` is applied as an idle timeout between chunks (inside collect).
        // - A separate overall timeout bounds a single download attempt.
        let collect_future =
            Self::collect_stream_to_bytes(http, &self.cancel, Some(self.request_timeout), url_str);

        // Bound a *single attempt* to a reasonable total duration while allowing
        // slow-but-progressing streaming downloads to complete.
        let total_timeout = self
            .request_timeout
            .saturating_mul(self.max_retries.saturating_add(1));

        match timeout(total_timeout, collect_future).await {
            Ok(res) => res,
            Err(_) => Err(HlsError::timeout(url_str.to_string())),
        }
    }

    // ----------------------------
    // Internals: retry policy
    // ----------------------------

    async fn retry_with_backoff<T, F, Fut>(
        &self,
        url: &str,
        op_name: &str,
        mut f: F,
    ) -> HlsResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = HlsResult<T>>,
    {
        let mut last_error: Option<HlsError> = None;
        let mut delay = self.retry_base_delay;

        for attempt in 0..=self.max_retries {
            if self.cancel.is_cancelled() {
                return Err(HlsError::Cancelled);
            }

            match f().await {
                Ok(v) => {
                    if attempt > 0 {
                        debug!(
                            url = url,
                            attempts = attempt + 1,
                            operation = op_name,
                            "download succeeded after retry"
                        );
                    }
                    return Ok(v);
                }
                Err(e) => {
                    debug!(
                        url = url,
                        attempt = attempt + 1,
                        max_attempts = self.max_retries + 1,
                        operation = op_name,
                        "download attempt failed: {}",
                        e
                    );
                    last_error = Some(e);

                    if attempt < self.max_retries {
                        tokio::select! {
                            biased;
                            _ = self.cancel.cancelled() => return Err(HlsError::Cancelled),
                            _ = tokio::time::sleep(delay) => {},
                        }
                        delay = (delay * 2).min(self.max_retry_delay);
                    }
                }
            }
        }

        debug!(
            url = url,
            attempts = self.max_retries + 1,
            operation = op_name,
            "download giving up after retries"
        );

        Err(last_error.unwrap_or_else(|| HlsError::io("download failed with no error")))
    }

    // ----------------------------
    // Internals: request attempts
    // ----------------------------

    async fn try_download_once(&self, url: &str) -> HlsResult<Bytes> {
        let url_parsed = Self::parse_url(url)?;
        let url_str = url_parsed.to_string();
        let http = self.create_stream(url_parsed).await?;
        self.download_via_stream(&url_str, http).await
    }

    fn build_key_headers(&self) -> HlsResult<HeaderMap> {
        let mut headers = HeaderMap::new();

        if let Some(h) = &self.key_request_headers {
            for (k, v) in h {
                let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                    HlsError::io_kind(
                        std::io::ErrorKind::InvalidInput,
                        format!("invalid header name `{}`: {}", k, e),
                    )
                })?;
                let value = HeaderValue::from_str(v).map_err(|e| {
                    HlsError::io_kind(
                        std::io::ErrorKind::InvalidInput,
                        format!("invalid header value for `{}`: {}", k, e),
                    )
                })?;
                headers.insert(name, value);
            }
        }

        Ok(headers)
    }

    async fn try_download_key_once(&self, url: &str) -> HlsResult<Bytes> {
        let url_parsed = Self::parse_url(url)?;
        let url_str = url_parsed.to_string();

        let headers = self.build_key_headers()?;

        // Create `HttpStream` using the shared client, but with custom request headers.
        // Race creation with cancellation.
        let create_fut =
            HttpStream::<ReqwestClient>::create_with_headers(url_parsed.clone(), headers);

        let http = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(HlsError::Cancelled),
            res = timeout(self.request_timeout, create_fut) => {
                match res {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(e)) => {
                        let msg = e.decode_error().await;
                        return Err(HlsError::http_stream_create_failed(msg));
                    }
                    Err(_) => return Err(HlsError::timeout(url_str)),
                }
            }
        };

        self.download_via_stream(&url_str, http).await
    }

    // ----------------------------
    // Internals: HTTP stream helpers
    // ----------------------------

    async fn create_stream(&self, url: Url) -> HlsResult<HttpStream<ReqwestClient>> {
        // Create the underlying HTTP stream using the shared client.
        // If creation fails, decode server-provided error text when possible.
        //
        // Creation is cancellable: if the downloader token is cancelled while we're
        // establishing the HTTP stream, abort early.
        let create_fut = timeout(
            self.request_timeout,
            HttpStream::<ReqwestClient>::create(url.clone()),
        );

        let res = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(HlsError::Cancelled),
            res = create_fut => res,
        };

        match res {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(e)) => {
                let msg = e.decode_error().await;
                Err(HlsError::http_stream_create_failed(msg))
            }
            Err(_) => Err(HlsError::timeout(url.to_string())),
        }
    }

    fn map_stream_errors(
        url: String,
        stream: HttpStream<ReqwestClient>,
    ) -> impl Stream<Item = Result<Bytes, HlsError>> + Send {
        let url: Arc<str> = Arc::from(url);
        stream.filter_map(move |res| {
            let url = Arc::clone(&url);
            async move {
                match res {
                    Ok(StreamMsg::Data(bytes)) => Some(Ok(bytes)),
                    Ok(StreamMsg::Control(_)) => None,
                    Err(e) => Some(Err(HlsError::io(format!(
                        "stream read error (url={}): {}",
                        url, e
                    )))),
                }
            }
        })
    }

    async fn collect_stream_to_bytes<S, E>(
        mut stream: S,
        cancel: &CancellationToken,
        idle_timeout: Option<Duration>,
        url: &str,
    ) -> HlsResult<Bytes>
    where
        S: Stream<Item = Result<StreamMsg, E>> + Unpin,
        E: Display,
    {
        let mut buf = BytesMut::with_capacity(16 * 1024);

        loop {
            // Support cancellation while reading, and optionally enforce an idle timeout
            // (no new chunks within the timeout window).
            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(HlsError::Cancelled),
                item = async {
                    if let Some(d) = idle_timeout {
                        match tokio::time::timeout(d, stream.next()).await {
                            Ok(v) => Ok(v),
                            Err(_) => Err(()),
                        }
                    } else {
                        Ok(stream.next().await)
                    }
                } => {
                    match item {
                        Ok(v) => v,
                        Err(()) => return Err(HlsError::timeout(url.to_string())),
                    }
                },
            };

            match next {
                Some(Ok(StreamMsg::Data(chunk))) => {
                    buf.extend_from_slice(&chunk);
                }
                Some(Ok(StreamMsg::Control(_))) => {
                    // Ignore control messages; this helper collects payload bytes only.
                    continue;
                }
                Some(Err(e)) => {
                    return Err(HlsError::io(e.to_string()));
                }
                None => break,
            }
        }

        Ok(buf.freeze())
    }

    // ----------------------------
    // Public API: metadata probes
    // ----------------------------

    /// Best-effort content length probe.
    ///
    /// Uses a minimal `Range: bytes=0-0` request when possible; otherwise falls back to response metadata.
    pub async fn probe_content_length(&self, url: &str) -> HlsResult<Option<u64>> {
        self.probe_content_length_inner(url, None).await
    }

    /// Cancellable variant of [`probe_content_length`].
    ///
    /// Cancelled when either the downloader token or `cancel` is cancelled.
    pub async fn probe_content_length_cancellable(
        &self,
        url: &str,
        cancel: &CancellationToken,
    ) -> HlsResult<Option<u64>> {
        self.probe_content_length_inner(url, Some(cancel)).await
    }

    async fn probe_content_length_inner(
        &self,
        url: &str,
        cancel: Option<&CancellationToken>,
    ) -> HlsResult<Option<u64>> {
        let url_parsed = Self::parse_url(url)?;
        let url_str = url_parsed.to_string();

        // Try a 0-0 range request first (minimal data, HEAD-like).
        let client = <ReqwestClient as stream_download::http::Client>::create();
        let range_fut = <ReqwestClient as stream_download::http::Client>::get_range(
            &client,
            &url_parsed,
            0,
            Some(0),
        );

        // Apply timeout and cancellation (downloader token OR optional caller token).
        let response_res = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(HlsError::Cancelled),
            _ = async {
                if let Some(token) = cancel {
                    token.cancelled().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => return Err(HlsError::Cancelled),
            res = tokio::time::timeout(self.request_timeout, range_fut) => res,
        };

        match response_res {
            Ok(Ok(response)) => {
                let status = response.status();
                if !status.is_success() {
                    return Err(HlsError::HttpError {
                        status: status.as_u16(),
                        url: url_str,
                    });
                }

                let headers = response.headers();

                // Prefer Content-Range for total length.
                if let Some(cr) = headers.get("Content-Range").and_then(|v| v.to_str().ok()) {
                    if let Some(total) = Self::parse_content_range_total(cr) {
                        return Ok(Some(total));
                    }
                }

                // Fallback: `Content-Length` might be the full length in a `200 OK` response.
                if let Some(cl) = headers
                    .get("Content-Length")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    if status.as_u16() == 200 {
                        return Ok(Some(cl));
                    }
                }

                // Last resort: create an `HttpStream` and read content_length() from metadata.
                let create_fut = tokio::time::timeout(
                    self.request_timeout,
                    HttpStream::<ReqwestClient>::create(url_parsed.clone()),
                );

                let create_res = tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => return Err(HlsError::Cancelled),
                    _ = async {
                        if let Some(token) = cancel {
                            token.cancelled().await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    } => return Err(HlsError::Cancelled),
                    res = create_fut => res,
                };

                let http = match create_res {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(e)) => {
                        let msg = e.decode_error().await;
                        return Err(HlsError::http_stream_create_failed_during_probe(msg));
                    }
                    Err(_) => return Err(HlsError::timeout(url_str)),
                };

                let cl_opt: Option<u64> = http.content_length().into();
                Ok(cl_opt)
            }
            Ok(Err(e)) => Err(HlsError::io(e.to_string())),
            Err(_) => Err(HlsError::timeout(url_str)),
        }
    }

    // ----------------------------
    // Internals: small parsing helpers
    // ----------------------------

    fn parse_url(url: &str) -> HlsResult<Url> {
        Url::parse(url).map_err(HlsError::url_parse)
    }

    /// Parse the `Content-Range` header to extract the total length.
    /// Expected formats:
    /// - "bytes 0-0/12345"
    /// - "bytes */12345"
    fn parse_content_range_total(header_val: &str) -> Option<u64> {
        // Find the '/' separator and parse the number after it.
        let idx = header_val.rfind('/')?;
        let total_str = header_val.get(idx + 1..)?.trim();
        if total_str == "*" {
            None
        } else {
            total_str.parse::<u64>().ok()
        }
    }
}
