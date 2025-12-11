//! HTTP backend: seek-gated MediaSource wrapper for `stream-download` readers,
//! and async packet producer for the unified pipeline.
//!
//! This module provides:
//! 1) A simple adapter to feed Symphonia (dev-0.6) with data coming from
//!    `stream-download` for regular HTTP resources while initially disabling
//!    seeking during probe and enabling it afterwards.
//! 2) An async packet producer that emits self-contained Packets into an
//!    async ring buffer, compatible with the new PipelineRunner.
//!
//! Why seek-gated?
//! - Symphonia's format probing may try to read from the end (e.g., to detect
//!   ID3v1). When the source is a network stream still being fetched, that
//!   would stall until the whole file is downloaded.
//! - Declaring the source as temporarily non-seekable helps Symphonia probe
//!   the format progressively without tail reads, so playback can start earlier.
//!   Seeking can then be enabled once the decoder is initialized.
//!
//! Usage (legacy):
//! - Call `open_http_seek_gated_mss(url)` to obtain a `(MediaSourceStream, SeekGateHandle)`.
//! - Pass `MediaSourceStream` to Symphonia's probe APIs to create a decoder.
//! - After probe, call `SeekGateHandle::enable_seek()` to allow seeking.
//!
//! Usage (pipeline):
//! - Call `run_http_packet_producer(url, out).await` to push Packets into the ByteRing.
//!
//! Note:
//! - This is a building block; higher-level pipeline code should drive decoding,
//!   resampling (rubato), and buffering.

use async_ringbuf::AsyncHeapProd;
use async_ringbuf::traits::producer::AsyncProducer;
use bytes::Bytes;
use std::io::Result as IoResult;
use stream_download::source::DecodeError;
use stream_download::storage::temp::TempStorageProvider;
use stream_download::{Settings, StreamDownload};

use crate::pipeline::Packet;

/// Async HTTP packet producer for the unified pipeline.
/// Emits self-contained Packet { init_hash, init_bytes, media_bytes } into `out`.
///
/// For plain HTTP, we don't have a formal init segment. We synthesize:
/// - init_hash = 0,
/// - init_bytes = empty,
/// and stream the incoming bytes as media_bytes chunks.
///
/// The chunk size is implementation-defined; we use a moderate size to balance latency and throughput.
pub async fn run_http_packet_producer(url: &str, mut out: AsyncHeapProd<Packet>) -> IoResult<()> {
    use stream_download::http::HttpStream;
    let parsed = reqwest::Url::parse(url).map_err(|e| io_other(&e.to_string()))?;
    let mut reader = match StreamDownload::new_http(
        parsed,
        TempStorageProvider::default(),
        Settings::<HttpStream<reqwest::Client>>::default(),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            let msg = e.decode_error().await;
            return Err(io_other(&msg));
        }
    };

    // Read a moderate chunk size; adjust if needed.
    loop {
        let (res, tmp, r_back) = tokio::task::spawn_blocking({
            let mut buf_guard = vec![0u8; 64 * 1024];
            let mut reader_inner = reader;
            move || {
                // Note: StreamDownload Reader implements blocking Read; bridge via spawn_blocking.
                // We use a separate buffer to avoid aliasing issues.
                use std::io::Read;
                let res = reader_inner.read(&mut buf_guard[..]);
                (res, buf_guard, reader_inner)
            }
        })
        .await
        .map_err(|join_err| io_other(&format!("join error: {join_err}")))?;
        reader = r_back;

        let n = match res {
            Ok(n) => n,
            Err(e) => return Err(e),
        };

        if n == 0 {
            break;
        }

        let pkt = Packet {
            init_hash: 0,
            init_bytes: Bytes::new(),
            media_bytes: Bytes::copy_from_slice(&tmp[..n]),
            variant_index: None,
            segment_duration: std::time::Duration::ZERO,
            segment_sequence: 0,
        };

        if out.push(pkt).await.is_err() {
            // Consumer dropped.
            break;
        }
    }

    Ok(())
}

fn io_other(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, msg.to_string())
}
