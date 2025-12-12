//! Unified packet producer abstraction for different backends.
//!
//! This module defines a common trait for packet producers that can be used
//! with the audio pipeline, allowing different backends (HTTP, HLS, etc.)
//! to be used interchangeably.
//!
//! This module groups source backends that feed the decoding pipeline:
//! - `http`: seek-gated MediaSource wrapper for `stream-download` HTTP resources, designed
//!           to avoid tail-probing during format detection and enable seeking post-probe.
//! - `hls`:  streaming MediaSource that feeds init + media segments from `HlsManager` to
//!           Symphonia in a blocking fashion (non-seekable for now).
//!
//! Additional backends (e.g., S3/opendal, process, async_read) can be added here in the future.

use async_trait::async_trait;
use kanal::AsyncSender;
use tokio_util::sync::CancellationToken;

use crate::pipeline::Packet;

pub mod hls;
pub mod http;

/// Trait for packet producers that generate audio data packets.
///
/// Packet producers are responsible for fetching audio data from a source
/// (HTTP, HLS, etc.) and sending it as `Packet` objects through an async channel.
#[async_trait]
pub trait PacketProducer: Send + Sync {
    /// Run the packet producer, sending packets through the provided channel.
    ///
    /// This method should run until the source is exhausted, an error occurs,
    /// or the cancellation token is triggered (if provided).
    ///
    /// # Arguments
    /// * `out` - Channel to send packets through
    /// * `cancel` - Optional cancellation token to stop the producer
    async fn run(
        &mut self,
        out: AsyncSender<Packet>,
        cancel: Option<CancellationToken>,
    ) -> std::io::Result<()>;
}

/// HTTP packet producer implementation.
pub struct HttpPacketProducer {
    url: String,
}

impl HttpPacketProducer {
    /// Create a new HTTP packet producer.
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

#[async_trait]
impl PacketProducer for HttpPacketProducer {
    async fn run(
        &mut self,
        out: AsyncSender<Packet>,
        cancel: Option<CancellationToken>,
    ) -> std::io::Result<()> {
        use crate::backends::http::run_http_packet_producer;

        // Check for cancellation before starting
        if let Some(cancel_token) = &cancel {
            if cancel_token.is_cancelled() {
                return Ok(());
            }
        }

        // Run the HTTP packet producer
        run_http_packet_producer(&self.url, out).await
    }
}

/// HLS packet producer implementation.
pub struct HlsPacketProducer {
    url: String,
    hls_config: stream_download_hls::HlsConfig,
    abr_config: stream_download_hls::AbrConfig,
    selection_mode: stream_download_hls::SelectionMode,
}

impl HlsPacketProducer {
    /// Create a new HLS packet producer.
    pub fn new(
        url: impl Into<String>,
        hls_config: stream_download_hls::HlsConfig,
        abr_config: stream_download_hls::AbrConfig,
        selection_mode: stream_download_hls::SelectionMode,
    ) -> Self {
        Self {
            url: url.into(),
            hls_config,
            abr_config,
            selection_mode,
        }
    }
}

#[async_trait]
impl PacketProducer for HlsPacketProducer {
    async fn run(
        &mut self,
        out: AsyncSender<Packet>,
        cancel: Option<CancellationToken>,
    ) -> std::io::Result<()> {
        use crate::backends::hls::run_hls_packet_producer;

        // Create cancellation token if not provided
        let cancel_token = cancel.unwrap_or_else(CancellationToken::new);

        // Run the HLS packet producer (it doesn't return a Result)
        run_hls_packet_producer(
            self.url.clone(),
            self.hls_config.clone(),
            self.abr_config.clone(),
            self.selection_mode,
            out,
            cancel_token,
        )
        .await;

        // Always return Ok since run_hls_packet_producer doesn't return a Result
        Ok(())
    }
}
