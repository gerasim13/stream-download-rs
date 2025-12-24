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
