//! HTTP backend: seek-gated MediaSource wrapper for `stream-download` readers.
//!
//! This module provides a simple adapter to feed Symphonia (dev-0.6) with data
//! coming from `stream-download` for regular HTTP resources while initially
//! disabling seeking during probe and enabling it afterwards.
//!
//! Why seek-gated?
//! - Symphonia's format probing may try to read from the end (e.g., to detect
//!   ID3v1). When the source is a network stream still being fetched, that
//!   would stall until the whole file is downloaded.
//! - Declaring the source as temporarily non-seekable helps Symphonia probe
//!   the format progressively without tail reads, so playback can start earlier.
//!   Seeking can then be enabled once the decoder is initialized.
//!
//! Usage:
//! - Call `open_http_seek_gated_mss(url)` to obtain a `(MediaSourceStream, SeekGateHandle)`.
//! - Pass `MediaSourceStream` to Symphonia's probe APIs to create a decoder.
//! - After probe, call `SeekGateHandle::enable_seek()` to allow seeking.
//!
//! Note:
//! - This is a building block; higher-level pipeline code should drive decoding,
//!   resampling (rubato), and buffering.

use std::io::{Read, Result as IoResult, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use stream_download::source::DecodeError;
use stream_download::storage::temp::TempStorageProvider;
use stream_download::{Settings, StreamDownload};
use symphonia::core::io::{MediaSource, MediaSourceStream, MediaSourceStreamOptions};

/// Seek-gated `MediaSource` backed by a `stream-download` HTTP reader.
///
/// Internally wraps the blocking `Read`/`Seek` implementation provided by `stream-download`.
/// Synchronization is handled through a `Mutex` so that the type is `Send + Sync`
/// as required by Symphonia's `MediaSource` trait.
///
/// Note: we can't put `Send` in the trait object bounds (Rust requires auto-traits to be
/// automatically inferred), so we ensure `Send` by construction.
pub struct HttpMediaSource {
    inner: Mutex<Box<dyn ReadSeek>>,
    seek_enabled: Arc<AtomicBool>,
}

/// Local trait to unify Read + Seek bounds for trait objects.
pub trait ReadSeek: Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeek for T {}

impl HttpMediaSource {
    /// Construct a new `HttpMediaSource` from a boxed reader with a seek gate.
    pub fn new(reader: Box<dyn ReadSeek>, seek_enabled: Arc<AtomicBool>) -> Self {
        Self {
            inner: Mutex::new(reader),
            seek_enabled,
        }
    }

    /// Convenience: create from a `stream-download` URL with default settings.
    pub async fn from_url(url: &str) -> IoResult<Self> {
        Self::from_url_with(url, Settings::default()).await
    }

    /// Convenience: create from a `stream-download` URL with custom settings.
    pub async fn from_url_with(
        url: &str,
        settings: Settings<stream_download::http::HttpStream<reqwest::Client>>,
    ) -> IoResult<Self> {
        let url = reqwest::Url::parse(url).map_err(|e| io_other(&e.to_string()))?;
        let reader =
            match StreamDownload::new_http(url, TempStorageProvider::default(), settings).await {
                Ok(r) => r,
                Err(e) => {
                    let msg = e.decode_error().await;
                    return Err(io_other(&msg));
                }
            };

        Ok(Self::new(
            Box::new(reader) as Box<dyn ReadSeek>,
            Arc::new(AtomicBool::new(false)),
        ))
    }
}

impl Read for HttpMediaSource {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        // `Read` requires &mut self; we still use the mutex to make the type Sync.
        let mut guard = self.inner.lock().expect("reader mutex poisoned");
        Read::read(&mut *guard, buf)
    }
}

impl Seek for HttpMediaSource {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        // Disallow seeking while the gate is disabled to avoid tail-probing during probe.
        if !self.seek_enabled.load(Ordering::Relaxed) {
            return Err(io_other("seek disabled during probe"));
        }
        let mut guard = self.inner.lock().expect("reader mutex poisoned");
        Seek::seek(&mut *guard, pos)
    }
}

impl MediaSource for HttpMediaSource {
    fn is_seekable(&self) -> bool {
        // Gated: disabled during probe, can be enabled afterwards.
        self.seek_enabled.load(Ordering::Relaxed)
    }

    fn byte_len(&self) -> Option<u64> {
        // Unknown length when streaming; `None` further discourages tail operations.
        None
    }
}

/// Handle to control the seek gate of a `HttpMediaSource`.
pub struct SeekGateHandle {
    flag: Arc<AtomicBool>,
}

impl SeekGateHandle {
    /// Enable seek on the underlying `HttpMediaSource` (typically after probe).
    pub fn enable_seek(&self) {
        self.flag.store(true, Ordering::Relaxed);
    }
}

/// Open a seek-gated `MediaSourceStream` for the given URL with a best-effort `Hint`.
///
/// - The extension (if any) is extracted from the URL and set on the `Hint`.
/// - The returned `MediaSourceStream` is created with default options.
/// - Seeking is disabled initially and can be enabled via the returned `SeekGateHandle`.
///
/// This is intended to be passed to Symphonia's probe API alongside the `Hint`.
pub async fn open_http_seek_gated_mss(url: &str) -> IoResult<(MediaSourceStream, SeekGateHandle)> {
    open_http_seek_gated_mss_with(url, Settings::default()).await
}

/// Same as `open_http_nonseekable_mss`, but allows providing custom `stream-download` `Settings`.
pub async fn open_http_seek_gated_mss_with(
    url: &str,
    settings: Settings<stream_download::http::HttpStream<reqwest::Client>>,
) -> IoResult<(MediaSourceStream, SeekGateHandle)> {
    // Initialize the seek gate disabled; will be enabled after probe by the caller.
    let seek_flag = Arc::new(AtomicBool::new(false));

    // Create the underlying stream-download reader.
    let parsed = reqwest::Url::parse(url).map_err(|e| io_other(&e.to_string()))?;
    let reader =
        match StreamDownload::new_http(parsed, TempStorageProvider::default(), settings).await {
            Ok(r) => r,
            Err(e) => {
                let msg = e.decode_error().await;
                return Err(io_other(&msg));
            }
        };

    // Create a seek-gated media source.
    let source = HttpMediaSource::new(Box::new(reader) as Box<dyn ReadSeek>, seek_flag.clone());

    // Wrap in a `MediaSourceStream` with default options.
    let mss = MediaSourceStream::new(
        Box::new(source) as Box<dyn MediaSource>,
        MediaSourceStreamOptions::default(),
    );

    let handle = SeekGateHandle { flag: seek_flag };
    Ok((mss, handle))
}

/// Build a lowercase file extension from the URL (used to aid format selection).
pub fn extension_from_url(url: &str) -> Option<String> {
    if let Some(ext) = url
        .rsplit_once('.')
        .map(|(_, ext)| ext)
        .or_else(|| url.split('.').last())
    {
        // Sanitize: strip query/fragment after extension if present.
        let ext = ext.split(|c| c == '?' || c == '#').next().unwrap_or(ext);
        if !ext.is_empty() {
            return Some(ext.to_ascii_lowercase());
        }
    }
    None
}

fn io_other(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, msg.to_string())
}
