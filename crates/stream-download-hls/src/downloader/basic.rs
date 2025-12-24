use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::stream::{BoxStream, Stream, StreamExt};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::error::{HlsError, HlsResult};

// Reuse the StreamDownload HTTP layer and its single, shared client.
// We intentionally avoid using reqwest directly here.
use stream_download::http::HttpStream;
use stream_download::http::reqwest::Client as ReqwestClient;
use stream_download::http::reqwest::Url;
use stream_download::source::{DecodeError, SourceStream, StreamMsg};

/// Async HTTP resource downloader optimized for HLS streaming.
///
/// This downloader delegates HTTP I/O to `stream-download`'s `HttpStream` to:
/// - reuse a shared reqwest client (connection pooling, DNS cache),
/// - provide streaming responses for media segments (chunked delivery),
/// - support HTTP byte-range requests for seeking within segments.
///
/// It also preserves simple "download fully" APIs for small resources like
/// playlists and encryption keys (returned as `Bytes` instead of `Vec<u8>`).
#[derive(Debug, Clone)]
pub struct ResourceDownloader {
    // Flattened configuration for requests and retry behavior.
    request_timeout: Duration,
    max_retries: u32,
    retry_base_delay: Duration,
    max_retry_delay: Duration,

    // Cancellation token used for ALL network operations.
    // Making this mandatory eliminates duplication between cancellable and non-cancellable APIs.
    cancel: CancellationToken,

    // Optional headers to attach to KEY fetch requests (AES-128 DRM flows).
    key_request_headers: Option<HashMap<String, String>>,
}

impl ResourceDownloader {
    /// Create a new downloader with the given configuration values.
    ///
    /// A cancellation token is mandatory and will be used for all network operations.
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

    /// Set optional headers to be sent with AES key fetch requests.
    pub fn with_key_request_headers(
        mut self,
        headers: Option<std::collections::HashMap<String, String>>,
    ) -> Self {
        self.key_request_headers = headers;
        self
    }

    /// Access the cancellation token used by this downloader.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    // ----------------------------
    // Byte-oriented helper methods
    // ----------------------------

    /// Download bytes from a URL.
    ///
    /// Includes automatic retry with exponential backoff.
    /// This operation is always cancellable via the downloader's token.
    pub async fn download_bytes(&self, url: &str) -> HlsResult<Bytes> {
        self.download_bytes_inner(url).await
    }

    /// Download a playlist file fully into memory.
    pub async fn download_playlist(&self, url: &str) -> HlsResult<Bytes> {
        self.download_bytes(url).await
    }

    /// Download an encryption key fully into memory.
    ///
    /// If `key_request_headers` is set, those headers are attached to the request.
    pub async fn download_key(&self, url: &str) -> HlsResult<Bytes> {
        self.download_key_inner(url).await
    }

    // ----------------------------
    // Streaming segment methods
    // ----------------------------

    /// Open a streaming HTTP connection for a media segment.
    ///
    /// Returns a boxed stream of chunks. The stream yields `Bytes` as they arrive.
    /// Errors are mapped into `HlsError`.
    pub async fn stream_segment(
        &self,
        url: &str,
    ) -> HlsResult<BoxStream<'static, Result<Bytes, HlsError>>> {
        let url = parse_url(url)?;
        let http = self.create_stream(url).await?;
        Ok(Self::map_stream_errors(http).boxed())
    }

    /// Open a streaming HTTP connection for a media segment with a byte range.
    ///
    /// The `end` is exclusive (i.e., `start..end`). Pass `None` for an open-ended
    /// range to the end of the resource.
    pub async fn stream_segment_range(
        &self,
        url: &str,
        start: u64,
        end: Option<u64>,
    ) -> HlsResult<BoxStream<'static, Result<Bytes, HlsError>>> {
        let url = parse_url(url)?;
        let mut http = self.create_stream(url).await?;
        // `HttpStream` expects an exclusive end value; this matches our API.
        http.seek_range(start, end)
            .await
            .map_err(|e| HlsError::Io(e))?;
        Ok(Self::map_stream_errors(http).boxed())
    }

    // ----------------------------
    // Internals
    // ----------------------------

    async fn download_bytes_inner(&self, url: &str) -> HlsResult<Bytes> {
        let mut last_error: Option<HlsError> = None;
        let mut delay = self.retry_base_delay;

        for attempt in 0..=self.max_retries {
            // Check cancellation before each attempt
            if self.cancel.is_cancelled() {
                return Err(HlsError::Cancelled);
            }

            match self.try_download_once(url).await {
                Ok(bytes) => {
                    if attempt > 0 {
                        debug!(
                            url = url,
                            attempts = attempt + 1,
                            "download succeeded after retry"
                        );
                    }
                    return Ok(bytes);
                }
                Err(e) => {
                    last_error = Some(e);

                    // Don't sleep after the last attempt
                    if attempt < self.max_retries {
                        // Sleep with cancellation support
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

        Err(last_error.unwrap_or_else(|| HlsError::io("download failed with no error")))
    }

    async fn download_key_inner(&self, url: &str) -> HlsResult<Bytes> {
        // Fast path: no custom headers => reuse the existing bytes downloader.
        if self.key_request_headers.is_none() {
            return self.download_bytes_inner(url).await;
        }

        // Header-aware path with retries/backoff + cancellation.
        let mut last_error: Option<HlsError> = None;
        let mut delay = self.retry_base_delay;

        for attempt in 0..=self.max_retries {
            // Check cancellation before each attempt
            if self.cancel.is_cancelled() {
                return Err(HlsError::Cancelled);
            }

            match self.try_download_key_once(url).await {
                Ok(bytes) => {
                    if attempt > 0 {
                        debug!(
                            url = url,
                            attempts = attempt + 1,
                            "key download succeeded after retry"
                        );
                    }
                    return Ok(bytes);
                }
                Err(e) => {
                    last_error = Some(e);

                    // Don't sleep after the last attempt
                    if attempt < self.max_retries {
                        // Sleep with cancellation support
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

        Err(last_error.unwrap_or_else(|| HlsError::io("key download failed with no error")))
    }

    async fn try_download_once(&self, url: &str) -> HlsResult<Bytes> {
        let url_parsed = parse_url(url)?;
        let url_str = url_parsed.to_string();
        let http = self.create_stream(url_parsed).await?;

        // Collect all chunks with a global timeout and cancellation.
        let collect_future = Self::collect_stream_to_bytes(http, &self.cancel);
        match timeout(self.request_timeout, collect_future).await {
            Ok(res) => res,
            Err(_) => Err(HlsError::timeout(url_str)),
        }
    }

    async fn try_download_key_once(&self, url: &str) -> HlsResult<Bytes> {
        let url_parsed = parse_url(url)?;
        let url_str = url_parsed.to_string();

        let mut headers = HeaderMap::new();
        if let Some(h) = &self.key_request_headers {
            for (k, v) in h {
                let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                    HlsError::io_kind(std::io::ErrorKind::InvalidInput, e.to_string())
                })?;
                let value = HeaderValue::from_str(v).map_err(|e| {
                    HlsError::io_kind(std::io::ErrorKind::InvalidInput, e.to_string())
                })?;
                headers.insert(name, value);
            }
        }

        // Create HttpStream using the shared client, but with custom request headers.
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

        // Collect all chunks with a global timeout and cancellation.
        let collect_future = Self::collect_stream_to_bytes(http, &self.cancel);
        match timeout(self.request_timeout, collect_future).await {
            Ok(res) => res,
            Err(_) => Err(HlsError::timeout(url_parsed.to_string())),
        }
    }

    async fn create_stream(&self, url: Url) -> HlsResult<HttpStream<ReqwestClient>> {
        // Use the default shared client via HttpStream::<C>::create.
        // On failures, map the error into HlsError using the DecodeError API.
        match timeout(
            self.request_timeout,
            HttpStream::<ReqwestClient>::create(url.clone()),
        )
        .await
        {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(e)) => {
                // Decode the error text from the server if possible
                let msg = e.decode_error().await;
                Err(HlsError::http_stream_create_failed(msg))
            }
            Err(_) => Err(HlsError::timeout(url.to_string())),
        }
    }

    fn map_stream_errors(
        stream: HttpStream<ReqwestClient>,
    ) -> impl Stream<Item = Result<Bytes, HlsError>> + Send {
        stream.filter_map(|res| async move {
            match res {
                Ok(StreamMsg::Data(bytes)) => Some(Ok(bytes)),
                Ok(StreamMsg::Control(_)) => None,
                Err(e) => Some(Err(HlsError::io(e.to_string()))),
            }
        })
    }

    async fn collect_stream_to_bytes<S, E>(
        mut stream: S,
        cancel: &CancellationToken,
    ) -> HlsResult<Bytes>
    where
        S: Stream<Item = Result<StreamMsg, E>> + Unpin,
        E: Display,
    {
        let mut buf = BytesMut::with_capacity(16 * 1024);

        loop {
            // Support cancellation while reading
            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(HlsError::Cancelled),
                item = stream.next() => item,
            };

            match next {
                Some(Ok(StreamMsg::Data(chunk))) => {
                    buf.extend_from_slice(&chunk);
                }
                Some(Ok(StreamMsg::Control(_))) => {
                    // Ignore ordered control messages; this helper is used only to collect payload bytes.
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
}

// ----------------------------
// Helpers
// ----------------------------

fn parse_url(url: &str) -> HlsResult<Url> {
    Url::parse(url).map_err(HlsError::url_parse)
}

/// Parse the `Content-Range` header to extract the total length.
/// Expected formats:
/// - "bytes 0-0/12345"
/// - "bytes */12345"
fn parse_content_range_total(header_val: &str) -> Option<u64> {
    // Find the '/' separator and parse the number after it
    let idx = header_val.rfind('/')?;
    let total_str = header_val.get(idx + 1..)?.trim();
    if total_str == "*" {
        None
    } else {
        total_str.parse::<u64>().ok()
    }
}

impl ResourceDownloader {
    /// Probe the content length using a minimal Range request (HEAD-like).
    ///
    /// This method tries to avoid downloading the whole body by requesting only a single byte
    /// (`Range: bytes=0-0`). If the server supports ranges, we parse the `Content-Range` header
    /// to obtain the total length. If that fails, we fall back to a normal GET via `HttpStream`
    /// and read the `Content-Length` header.
    pub async fn probe_content_length(&self, url: &str) -> HlsResult<Option<u64>> {
        self.probe_content_length_inner(url, None).await
    }

    /// Cancellable variant of [`probe_content_length`].
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
        let url_parsed = parse_url(url)?;
        let url_str = url_parsed.to_string();

        // Try a 0-0 range request first (minimal data, HEAD-like).
        let client = <ReqwestClient as stream_download::http::Client>::create();
        let range_fut = <ReqwestClient as stream_download::http::Client>::get_range(
            &client,
            &url_parsed,
            0,
            Some(0),
        );

        // Apply timeout and optional cancellation.
        let response_res = if let Some(token) = cancel {
            tokio::select! {
                biased;
                _ = token.cancelled() => return Err(HlsError::Cancelled),
                res = tokio::time::timeout(self.request_timeout, range_fut) => res,
            }
        } else {
            tokio::time::timeout(self.request_timeout, range_fut).await
        };

        match response_res {
            Ok(Ok(response)) => {
                // If server doesn't support range, it may still return 200.
                // We'll try to parse Content-Range first; fallback to Content-Length.
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
                    if let Some(total) = parse_content_range_total(cr) {
                        return Ok(Some(total));
                    }
                }

                // Fallback: Content-Length might be the full length or 1 (for 0-0).
                if let Some(cl) = headers
                    .get("Content-Length")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    // If status is 200 OK, many servers include full Content-Length.
                    // If 206 Partial Content, Content-Length is the size of the partial body (likely 1).
                    // We can't infer total from this unless Content-Range was present.
                    if status.as_u16() == 200 {
                        return Ok(Some(cl));
                    }
                }

                // As a last resort, perform a normal GET via HttpStream and read content_length
                // from response headers (without consuming the body).
                let http = match tokio::time::timeout(
                    self.request_timeout,
                    HttpStream::<ReqwestClient>::create(url_parsed.clone()),
                )
                .await
                {
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
}
