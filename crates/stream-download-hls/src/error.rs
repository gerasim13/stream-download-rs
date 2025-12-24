//! Unified crate-level error types.
//!
//! This module provides a single [`HlsError`] type used across the crate and a
//! convenient [`HlsResult`] alias.
//!
//! Rationale
//! ---------
//! Historically this crate had two error layers:
//! - `HlsError` (playlist/parser/downloader level)
//! - `HlsStreamError` (stream/worker level, implementing `DecodeError`)
//!
//! These overlapped (`Cancelled`, IO-ish failures) and required wrapper enums.
//! The crate now uses a single error type that can be returned from both the
//! playlist/manager layers and the `SourceStream` implementation.
//!
//! Note: this error type intentionally remains small and string-based in some
//! variants to avoid pulling concrete HTTP client error types into the public API.

use std::io;

use stream_download::source::DecodeError;

/// Result type used by this crate.
pub type HlsResult<T> = Result<T, HlsError>;

/// Unified error type for the `stream-download-hls` crate.
#[derive(Debug, thiserror::Error)]
pub enum HlsError {
    /// A generic error with a message.
    #[error("{0}")]
    Message(String),

    /// Errors related to invalid or unsupported playlist contents.
    #[error("invalid playlist: {0}")]
    InvalidPlaylist(String),

    /// Invalid parameters provided by the caller.
    #[error("invalid parameters: {0}")]
    InvalidParams(&'static str),

    /// Operation was cancelled.
    #[error("operation cancelled")]
    Cancelled,

    /// I/O error.
    ///
    /// Uses the concrete `std::io::Error` to preserve error kinds and sources.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// HTTP request failed.
    #[error("HTTP error: {status} for {url}")]
    HttpError {
        /// HTTP status code.
        status: u16,
        /// URL that failed.
        url: String,
    },

    /// Request timed out.
    #[error("request timeout for {0}")]
    Timeout(String),

    /// No variants available in the master playlist.
    #[error("no variants available in master playlist")]
    NoVariants,

    /// Failed to send a seek command or perform seek.
    #[error("seek failed")]
    SeekFailed,

    /// Streaming loop not initialized.
    #[error("streaming loop not initialized")]
    StreamingLoopNotInitialized,

    /// Backward seek not supported.
    #[error("backward seek not supported")]
    BackwardSeekNotSupported,

    /// Extra context around a lower-level HLS error.
    ///
    /// Use this for adding human-readable context without creating many wrapper enums.
    #[error("{context}: {source}")]
    Context {
        /// What we were doing when the error occurred.
        context: &'static str,
        /// The underlying error.
        #[source]
        source: Box<HlsError>,
    },
}

impl HlsError {
    /// Convenience helper to construct a simple message error.
    pub fn msg(msg: impl Into<String>) -> Self {
        HlsError::Message(msg.into())
    }

    /// AES-128-CBC decryption failed.
    ///
    /// Intended for call-sites like: `.map_err(HlsError::aes128_cbc_decrypt_failed)?;`
    pub fn aes128_cbc_decrypt_failed<E>(e: E) -> Self
    where
        E: std::fmt::Display,
    {
        HlsError::msg(format!("AES-128-CBC decryption failed: {e}"))
    }

    /// Invalid AES-128 key length (expected 16 bytes).
    ///
    /// Intended for call-sites like: `return Err(HlsError::invalid_aes128_key_len(kb.len()));`
    pub fn invalid_aes128_key_len(got_len: usize) -> Self {
        HlsError::msg(format!(
            "invalid AES-128 key length: expected 16, got {}",
            got_len
        ))
    }

    /// Unable to determine the size for a segment while building a seek map.
    ///
    /// Intended for call-sites like: `return Err(HlsError::segment_size_unknown(seq));`
    pub fn segment_size_unknown(sequence: u64) -> Self {
        HlsError::msg(format!(
            "unable to determine size for segment sequence {}",
            sequence
        ))
    }

    /// Segment sequence is not present in the current playlist/window.
    ///
    /// Intended for call-sites like: `return Err(HlsError::segment_sequence_not_found(seq));`
    pub fn segment_sequence_not_found(sequence: u64) -> Self {
        HlsError::msg(format!(
            "segment sequence {} not found in current playlist",
            sequence
        ))
    }

    /// Convenience helper for invalid playlist errors.
    pub fn invalid_playlist(msg: impl Into<String>) -> Self {
        HlsError::InvalidPlaylist(msg.into())
    }

    /// Convenience helper for timeout errors.
    pub fn timeout(target: impl Into<String>) -> Self {
        HlsError::Timeout(target.into())
    }

    /// Convenience helper to create `HlsError::Io` with a specific `io::ErrorKind`.
    pub fn io_kind(kind: io::ErrorKind, msg: impl Into<String>) -> Self {
        HlsError::Io(io::Error::new(kind, msg.into()))
    }

    /// Convenience helper to create `HlsError::Io` with kind `Other`.
    ///
    /// Use this when you only have a displayable message and no real `io::Error`.
    pub fn io(msg: impl Into<String>) -> Self {
        HlsError::io_kind(io::ErrorKind::Other, msg)
    }

    /// HTTP stream creation failed (mapped as an `Other` IO error).
    ///
    /// Intended for call-sites that decode an upstream error into a string:
    /// `Err(HlsError::http_stream_create_failed(msg))`
    pub fn http_stream_create_failed(msg: impl Into<String>) -> Self {
        HlsError::io(format!("HTTP stream creation failed: {}", msg.into()))
    }

    /// HTTP stream creation failed during a probe (mapped as an `Other` IO error).
    pub fn http_stream_create_failed_during_probe(msg: impl Into<String>) -> Self {
        HlsError::io(format!(
            "HTTP stream creation failed during probe: {}",
            msg.into()
        ))
    }

    /// Parse a URL-like string failed (mapped as an `InvalidInput` IO error).
    ///
    /// Intended for call-sites like: `Url::parse(...).map_err(HlsError::url_parse)?;`
    pub fn url_parse(e: url::ParseError) -> Self {
        HlsError::io_kind(
            std::io::ErrorKind::InvalidInput,
            format!("invalid URL: {e}"),
        )
    }

    /// Parsing a base URL failed (mapped as an `InvalidInput` IO error).
    ///
    /// Intended for call-sites like: `Url::parse(base).map_err(HlsError::base_url_parse)?;`
    pub fn base_url_parse(e: url::ParseError) -> Self {
        HlsError::io_kind(
            std::io::ErrorKind::InvalidInput,
            format!("failed to parse base URL: {e}"),
        )
    }

    /// Joining a relative URL against a base failed (mapped as an `InvalidInput` IO error).
    ///
    /// Intended for call-sites like: `base.join(rel).map_err(HlsError::url_join)?;`
    pub fn url_join(e: url::ParseError) -> Self {
        HlsError::io_kind(
            std::io::ErrorKind::InvalidInput,
            format!("failed to join URL: {e}"),
        )
    }

    /// Invalid UTF-8 inside a playlist body.
    ///
    /// Intended for call-sites like: `from_utf8(...).map_err(HlsError::playlist_utf8)?;`
    pub fn playlist_utf8(e: std::str::Utf8Error) -> Self {
        HlsError::invalid_playlist(format!("invalid UTF-8: {e}"))
    }

    /// Playlist parse error coming from `hls_m3u8`.
    ///
    /// Intended for call-sites like:
    /// `HlsMasterPlaylist::try_from(...).map_err(HlsError::playlist_parse)?;`
    pub fn playlist_parse<E>(e: E) -> Self
    where
        E: std::fmt::Display,
    {
        HlsError::invalid_playlist(format!("hls_m3u8 parse error: {e}"))
    }

    /// Attach static context to an existing error.
    pub fn with_context(self, context: &'static str) -> Self {
        HlsError::Context {
            context,
            source: Box::new(self),
        }
    }
}

impl DecodeError for HlsError {
    async fn decode_error(self) -> String {
        self.to_string()
    }
}
