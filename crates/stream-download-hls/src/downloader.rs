//! Async HTTP resource downloader for HLS streams.
//!
//! This module provides [`ResourceDownloader`], an efficient async HTTP client
//! wrapper optimized for HLS streaming. It uses `reqwest` directly instead of
//! `StreamDownload` to avoid overhead of temp file creation and blocking I/O
//! for small resources like playlists and segments.
//!
//! # Design Rationale
//!
//! HLS segments are typically small enough (64KB - 2MB for audio) to fit in memory.
//! Using `StreamDownload` for each segment would create unnecessary overhead:
//! - Temp file creation per segment
//! - `spawn_blocking` for synchronous `Read` trait
//! - Full lifecycle management for short-lived resources
//!
//! Instead, `ResourceDownloader` provides:
//! - Shared `reqwest::Client` with connection pooling
//! - Fully async downloads with no blocking calls
//! - Retry logic with exponential backoff
//! - Cancellation support via `CancellationToken`
//!
//! The `Read`/`Seek` functionality is provided at the `HlsStream` level where
//! `StreamDownload` wraps the entire concatenated HLS stream.

use std::time::Duration;

use bytes::Bytes;
use reqwest::Client;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::model::{HlsError, HlsResult};

/// Configuration for HTTP requests made by [`ResourceDownloader`].
#[derive(Debug, Clone)]
pub struct DownloaderConfig {
    /// Timeout for individual HTTP requests.
    /// Default: 30 seconds.
    pub request_timeout: Duration,

    /// Maximum number of retry attempts for failed requests.
    /// Default: 3 retries.
    pub max_retries: u32,

    /// Base delay for exponential backoff between retries.
    /// Default: 100ms.
    pub retry_base_delay: Duration,

    /// Maximum delay between retries (caps exponential growth).
    /// Default: 5 seconds.
    pub max_retry_delay: Duration,
}

impl Default for DownloaderConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_base_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(5),
        }
    }
}

impl DownloaderConfig {
    /// Create configuration optimized for mobile devices.
    ///
    /// Uses shorter timeouts and more aggressive retries to handle
    /// flaky mobile network conditions.
    pub fn mobile() -> Self {
        Self {
            request_timeout: Duration::from_secs(15),
            max_retries: 5,
            retry_base_delay: Duration::from_millis(50),
            max_retry_delay: Duration::from_secs(3),
        }
    }

    /// Create configuration for low-latency live streaming.
    ///
    /// Uses shorter timeouts and fewer retries to maintain low latency.
    pub fn low_latency() -> Self {
        Self {
            request_timeout: Duration::from_secs(5),
            max_retries: 1,
            retry_base_delay: Duration::from_millis(50),
            max_retry_delay: Duration::from_millis(500),
        }
    }
}

/// Async HTTP resource downloader optimized for HLS streaming.
///
/// This downloader uses `reqwest` directly for efficient async HTTP requests.
/// It maintains a shared HTTP client for connection pooling and provides
/// retry logic with exponential backoff.
///
/// # Example
///
/// ```no_run
/// use stream_download_hls::downloader::{ResourceDownloader, DownloaderConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let downloader = ResourceDownloader::new(DownloaderConfig::default());
///
/// // Download a playlist
/// let playlist_bytes = downloader.download_bytes("https://example.com/master.m3u8").await?;
///
/// // Download with cancellation support
/// use tokio_util::sync::CancellationToken;
/// let cancel = CancellationToken::new();
/// let segment = downloader
///     .download_bytes_cancellable("https://example.com/seg0.ts", &cancel)
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ResourceDownloader {
    /// Shared HTTP client - connection pooling is handled by reqwest internally
    client: Client,
    /// Configuration for requests
    config: DownloaderConfig,
}

impl ResourceDownloader {
    /// Create a new downloader with the given configuration.
    ///
    /// This creates a new `reqwest::Client` internally. For better connection
    /// reuse across multiple downloaders, use [`with_client`](Self::with_client).
    pub fn new(config: DownloaderConfig) -> Self {
        Self {
            client: Client::new(),
            config,
        }
    }

    /// Create a new downloader with a shared HTTP client.
    ///
    /// This is the recommended constructor when you have multiple components
    /// that need to make HTTP requests, as it enables connection pooling
    /// across all of them.
    pub fn with_client(client: Client, config: DownloaderConfig) -> Self {
        Self { client, config }
    }

    /// Create a downloader with a shared client and default configuration.
    pub fn default_with_client(client: Client) -> Self {
        Self::with_client(client, DownloaderConfig::default())
    }

    /// Get a reference to the underlying HTTP client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &DownloaderConfig {
        &self.config
    }

    /// Download bytes from a URL.
    ///
    /// This method includes automatic retry with exponential backoff.
    /// For downloads that should be cancellable, use
    /// [`download_bytes_cancellable`](Self::download_bytes_cancellable).
    pub async fn download_bytes(&self, url: &str) -> HlsResult<Bytes> {
        self.download_with_retry(url, None).await
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
        self.download_with_retry(url, Some(cancel)).await
    }

    /// Download a playlist file.
    ///
    /// This is a convenience alias for [`download_bytes`](Self::download_bytes)
    /// that makes the intent clearer in calling code.
    pub async fn download_playlist(&self, url: &str) -> HlsResult<Bytes> {
        self.download_bytes(url).await
    }

    /// Download a media segment.
    ///
    /// This is a convenience alias for [`download_bytes`](Self::download_bytes)
    /// that makes the intent clearer in calling code.
    pub async fn download_segment(&self, url: &str) -> HlsResult<Bytes> {
        self.download_bytes(url).await
    }

    /// Download an encryption key.
    ///
    /// This is a convenience alias for [`download_bytes`](Self::download_bytes)
    /// that makes the intent clearer in calling code.
    pub async fn download_key(&self, url: &str) -> HlsResult<Bytes> {
        self.download_bytes(url).await
    }

    /// Internal: download with retry logic and optional cancellation.
    async fn download_with_retry(
        &self,
        url: &str,
        cancel: Option<&CancellationToken>,
    ) -> HlsResult<Bytes> {
        let mut last_error = None;
        let mut delay = self.config.retry_base_delay;

        for attempt in 0..=self.config.max_retries {
            // Check cancellation before each attempt
            if let Some(token) = cancel {
                if token.is_cancelled() {
                    return Err(HlsError::Cancelled);
                }
            }

            match self.download_once(url).await {
                Ok(bytes) => {
                    if attempt > 0 {
                        tracing::debug!(
                            url = url,
                            attempts = attempt + 1,
                            "Download succeeded after retry"
                        );
                    }
                    return Ok(bytes);
                }
                Err(e) => {
                    tracing::warn!(
                        url = url,
                        attempt = attempt,
                        max_retries = self.config.max_retries,
                        error = %e,
                        "Download attempt failed"
                    );
                    last_error = Some(e);

                    // Don't sleep after the last attempt
                    if attempt < self.config.max_retries {
                        // Async delay with cancellation support
                        if let Some(token) = cancel {
                            tokio::select! {
                                biased;
                                _ = token.cancelled() => {
                                    return Err(HlsError::Cancelled);
                                }
                                _ = tokio::time::sleep(delay) => {}
                            }
                        } else {
                            tokio::time::sleep(delay).await;
                        }

                        // Exponential backoff with cap
                        delay = (delay * 2).min(self.config.max_retry_delay);
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| HlsError::Io("download failed with no error".into())))
    }

    /// Internal: single download attempt.
    async fn download_once(&self, url: &str) -> HlsResult<Bytes> {
        // Send request with timeout
        let response = timeout(self.config.request_timeout, self.client.get(url).send())
            .await
            .map_err(|_| HlsError::Timeout(url.to_string()))?
            .map_err(|e| HlsError::Io(format!("HTTP request failed: {}", e)))?;

        // Check status code
        let status = response.status();
        if !status.is_success() {
            return Err(HlsError::HttpError {
                status: status.as_u16(),
                url: url.to_string(),
            });
        }

        // Read body asynchronously
        timeout(self.config.request_timeout, response.bytes())
            .await
            .map_err(|_| HlsError::Timeout(url.to_string()))?
            .map_err(|e| HlsError::Io(format!("Failed to read response body: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_sane_values() {
        let config = DownloaderConfig::default();
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_base_delay, Duration::from_millis(100));
        assert_eq!(config.max_retry_delay, Duration::from_secs(5));
    }

    #[test]
    fn mobile_config_is_more_aggressive() {
        let config = DownloaderConfig::mobile();
        assert!(config.request_timeout < Duration::from_secs(30));
        assert!(config.max_retries > 3);
    }

    #[test]
    fn low_latency_config_has_short_timeouts() {
        let config = DownloaderConfig::low_latency();
        assert!(config.request_timeout <= Duration::from_secs(5));
        assert!(config.max_retries <= 2);
    }
}
