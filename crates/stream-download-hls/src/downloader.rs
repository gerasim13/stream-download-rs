use std::fmt::Display;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::stream::{BoxStream, Stream, StreamExt};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::model::{HlsError, HlsResult};

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
}

impl ResourceDownloader {
    /// Create a new downloader with the given configuration values.
    pub fn new(
        request_timeout: Duration,
        max_retries: u32,
        retry_base_delay: Duration,
        max_retry_delay: Duration,
    ) -> Self {
        Self {
            request_timeout,
            max_retries,
            retry_base_delay,
            max_retry_delay,
        }
    }

    // ----------------------------
    // Byte-oriented helper methods
    // ----------------------------

    /// Download bytes from a URL.
    ///
    /// Includes automatic retry with exponential backoff.
    pub async fn download_bytes(&self, url: &str) -> HlsResult<Bytes> {
        self.download_bytes_inner(url, None).await
    }

    /// Download bytes from a URL with cancellation support.
    ///
    /// The download will be aborted if the cancellation token is triggered,
    /// including during retry delays.
    pub async fn download_bytes_cancellable(
        &self,
        url: &str,
        cancel: &CancellationToken,
    ) -> HlsResult<Bytes> {
        self.download_bytes_inner(url, Some(cancel)).await
    }

    /// Download a playlist file fully into memory.
    pub async fn download_playlist(&self, url: &str) -> HlsResult<Bytes> {
        self.download_bytes(url).await
    }

    /// Download an encryption key fully into memory.
    pub async fn download_key(&self, url: &str) -> HlsResult<Bytes> {
        self.download_bytes(url).await
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
        http.seek_range(start, end).await.map_err(io_to_hls_error)?;
        Ok(Self::map_stream_errors(http).boxed())
    }

    // ----------------------------
    // Internals
    // ----------------------------

    async fn download_bytes_inner(
        &self,
        url: &str,
        cancel: Option<&CancellationToken>,
    ) -> HlsResult<Bytes> {
        let mut last_error: Option<HlsError> = None;
        let mut delay = self.retry_base_delay;

        for attempt in 0..=self.max_retries {
            // Check cancellation before each attempt
            if let Some(token) = cancel {
                if token.is_cancelled() {
                    return Err(HlsError::Cancelled);
                }
            }

            match self.try_download_once(url, cancel).await {
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
                        if let Some(token) = cancel {
                            tokio::select! {
                                biased;
                                _ = token.cancelled() => return Err(HlsError::Cancelled),
                                _ = tokio::time::sleep(delay) => {},
                            }
                        } else {
                            tokio::time::sleep(delay).await;
                        }
                        delay = (delay * 2).min(self.max_retry_delay);
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| HlsError::Io("download failed with no error".into())))
    }

    async fn try_download_once(
        &self,
        url: &str,
        cancel: Option<&CancellationToken>,
    ) -> HlsResult<Bytes> {
        let url_parsed = parse_url(url)?;
        let url_str = url_parsed.to_string();
        let http = self.create_stream(url_parsed).await?;

        // Collect all chunks with a global timeout and optional cancellation
        let collect_future = Self::collect_stream_to_bytes(http, cancel);
        match timeout(self.request_timeout, collect_future).await {
            Ok(res) => res,
            Err(_) => Err(HlsError::Timeout(url_str)),
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
                Err(HlsError::Io(format!("HTTP stream creation failed: {msg}")))
            }
            Err(_) => Err(HlsError::Timeout(url.to_string())),
        }
    }

    fn map_stream_errors(
        stream: HttpStream<ReqwestClient>,
    ) -> impl Stream<Item = Result<Bytes, HlsError>> + Send {
        stream.filter_map(|res| async move {
            match res {
                Ok(StreamMsg::Data(bytes)) => Some(Ok(bytes)),
                Ok(StreamMsg::Control(_)) => None,
                Err(e) => Some(Err(HlsError::Io(e.to_string()))),
            }
        })
    }

    async fn collect_stream_to_bytes<S, E>(
        mut stream: S,
        cancel: Option<&CancellationToken>,
    ) -> HlsResult<Bytes>
    where
        S: Stream<Item = Result<StreamMsg, E>> + Unpin,
        E: Display,
    {
        let mut buf = BytesMut::with_capacity(16 * 1024);

        loop {
            // Support cancellation while reading
            let next = if let Some(token) = cancel {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => return Err(HlsError::Cancelled),
                    item = stream.next() => item,
                }
            } else {
                stream.next().await
            };

            match next {
                Some(Ok(StreamMsg::Data(chunk))) => {
                    buf.extend_from_slice(&chunk);
                }
                Some(Ok(StreamMsg::Control(_))) => {
                    // Ignore ordered control messages; this helper is used only to collect payload bytes.
                    continue;
                }
                Some(Err(e)) => return Err(HlsError::Io(e.to_string())),
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
    Url::parse(url).map_err(|e| HlsError::Io(format!("invalid URL: {e}")))
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
                        return Err(HlsError::Io(format!(
                            "HTTP stream creation failed during probe: {msg}"
                        )));
                    }
                    Err(_) => return Err(HlsError::Timeout(url_str)),
                };

                let cl_opt: Option<u64> = http.content_length().into();
                Ok(cl_opt)
            }
            Ok(Err(e)) => Err(HlsError::Io(e.to_string())),
            Err(_) => Err(HlsError::Timeout(url_str)),
        }
    }
}

fn io_to_hls_error(e: std::io::Error) -> HlsError {
    HlsError::Io(e.to_string())
}
