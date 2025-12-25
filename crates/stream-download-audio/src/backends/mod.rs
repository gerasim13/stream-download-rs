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
//! - `common`: Common utilities shared between packet producers.
//!
//! Additional backends (e.g., S3/opendal, process, async_read) can be added here in the future.

use async_trait::async_trait;
use kanal::{AsyncReceiver, AsyncSender};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::pipeline::Packet;

/// Commands that can be sent to a packet producer.
#[derive(Debug, Clone)]
pub enum ProducerCommand {
    /// Seek to a specific position in the stream.
    Seek(Duration),
    /// Flush all buffered data and reset state.
    Flush,
}

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
    /// * `commands` - Optional channel to receive commands (seek, flush, etc.)
    /// * `cancel` - Optional cancellation token to stop the producer
    async fn run(
        &mut self,
        out: AsyncSender<Packet>,
        commands: Option<AsyncReceiver<ProducerCommand>>,
        cancel: Option<CancellationToken>,
    ) -> std::io::Result<()>;
}

pub mod common;
pub mod hls;
pub mod http;

// Re-export the concrete implementations for convenience
pub use self::hls::HlsPacketProducer;
pub use self::http::HttpPacketProducer;
