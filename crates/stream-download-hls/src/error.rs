//! Crate error type.
//!
//! This module defines [`HlsError`] and the [`HlsResult`] alias used across the crate.
//!
//! Implementation note: some variants are string-based to avoid exposing a specific HTTP client
//! error type in the public API.

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
    pub fn io(msg: impl Into<String>) -> Self {
        HlsError::io_kind(io::ErrorKind::Other, msg)
    }

    /// HTTP stream creation failed (mapped as an `Other` IO error).
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

    /// URL parse error (mapped as an `InvalidInput` I/O error).
    pub fn url_parse(e: url::ParseError) -> Self {
        HlsError::io_kind(
            std::io::ErrorKind::InvalidInput,
            format!("invalid URL: {e}"),
        )
    }

    /// Base URL parse error (mapped as an `InvalidInput` I/O error).
    pub fn base_url_parse(e: url::ParseError) -> Self {
        HlsError::io_kind(
            std::io::ErrorKind::InvalidInput,
            format!("failed to parse base URL: {e}"),
        )
    }

    /// URL join error (mapped as an `InvalidInput` I/O error).
    pub fn url_join(e: url::ParseError) -> Self {
        HlsError::io_kind(
            std::io::ErrorKind::InvalidInput,
            format!("failed to join URL: {e}"),
        )
    }

    /// Invalid UTF-8 inside a playlist body.
    pub fn playlist_utf8(e: std::str::Utf8Error) -> Self {
        HlsError::invalid_playlist(format!("invalid UTF-8: {e}"))
    }

    /// Playlist parse error coming from `hls_m3u8`.
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
