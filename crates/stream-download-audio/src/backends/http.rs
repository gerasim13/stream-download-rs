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

use async_trait::async_trait;
use bytes::Bytes;
use kanal::AsyncSender;
use reqwest::{Client, Url};
use std::io::Result as IoResult;

use stream_download::http::HttpStream;
use stream_download::source::DecodeError;

use stream_download::storage::temp::TempStorageProvider;
use stream_download::{Settings, StreamDownload};
use tokio_util::sync::CancellationToken;

use crate::backends::PacketProducer;
use crate::backends::common::{io_other, run_blocking_reading_loop};
use crate::pipeline::Packet;

/// HTTP packet producer implementation.
pub struct HttpPacketProducer {
    url: Url,
}

impl HttpPacketProducer {
    /// Create a new HTTP packet producer.
    pub fn new(url: Url) -> Self {
        Self { url }
    }
}

#[async_trait]
impl PacketProducer for HttpPacketProducer {
    /// Async HTTP packet producer for the unified pipeline.
    /// Emits self-contained Packet { init_hash, init_bytes, media_bytes } into `out`.
    ///
    /// For plain HTTP, we don't have a formal init segment. We synthesize:
    /// - init_hash = 0,
    /// - init_bytes = empty,
    /// and stream the incoming bytes as media_bytes chunks.
    ///
    /// The chunk size is implementation-defined; we use a moderate size to balance latency and throughput.
    async fn run(
        &mut self,
        out: AsyncSender<Packet>,
        _commands: Option<kanal::AsyncReceiver<crate::backends::ProducerCommand>>,
        cancel: Option<CancellationToken>,
        _on_event: Option<std::sync::Arc<dyn Fn(crate::PlayerEvent) + Send + Sync>>,
    ) -> IoResult<()> {
        // TODO: Implement seek support for HTTP streams
        // For now, commands are ignored
        let reader = match StreamDownload::new_http(
            self.url.clone(),
            TempStorageProvider::default(),
            Settings::<HttpStream<Client>>::default(),
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                let msg: String = e.decode_error().await;
                return Err(io_other(&msg));
            }
        };

        // Run the blocking reading loop in a separate thread
        let sync_out = out.clone_sync();
        tokio::task::spawn_blocking(move || {
            run_blocking_reading_loop(
                reader,
                sync_out,
                cancel,
                None,         // variant_index
                0,            // init_hash
                Bytes::new(), // init_bytes
            )
        })
        .await
        .map_err(|join_err| io_other(&format!("join error: {join_err}")))?
    }
}
