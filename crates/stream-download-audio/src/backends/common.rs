//! Common utilities for packet producers.
//!
//! This module provides shared functionality for different packet producer implementations,
//! such as the blocking reading loop and error handling utilities.

use bytes::Bytes;
use kanal::{AsyncSender, Sender};
use std::io::{self, Read, Result as IoResult};
use stream_download::StreamDownload;
use stream_download::storage::temp::TempStorageProvider;
use tokio_util::sync::CancellationToken;

use crate::pipeline::Packet;

/// Run a blocking reading loop that reads from a StreamDownload and sends packets.
///
/// This function is used by both HTTP and HLS packet producers to read data
/// from a StreamDownload instance and send it as Packet objects through a
/// synchronous channel.
///
/// # Arguments
/// * `reader` - The StreamDownload instance to read from
/// * `sync_out` - Synchronous channel to send packets through
/// * `cancel` - Optional cancellation token to stop the reading loop
/// * `variant_index` - Optional variant index to include in packets (for HLS)
/// * `init_hash` - Optional init segment hash to include in packets (for HLS)
/// * `init_bytes` - Optional init segment bytes to include in packets (for HLS)
pub fn run_blocking_reading_loop<P>(
    mut reader: StreamDownload<P>,
    sync_out: Sender<Packet>,
    cancel: Option<CancellationToken>,
    variant_index: Option<usize>,
    init_hash: u64,
    init_bytes: Bytes,
) -> IoResult<()>
where
    P: stream_download::storage::StorageProvider,
{
    let mut buf = vec![0u8; 256 * 1024]; // 256KB buffer
    let mut total_bytes = 0;
    let mut read_count = 0;

    loop {
        // Check for cancellation
        if let Some(cancel_token) = &cancel {
            if cancel_token.is_cancelled() {
                tracing::trace!("run_blocking_reading_loop: cancelled");
                return Ok(());
            }
        }

        let n = match reader.read(&mut buf[..]) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!("run_blocking_reading_loop: read error: {}", e);
                return Err(e);
            }
        };

        read_count += 1;
        total_bytes += n as u64;

        tracing::trace!(
            "run_blocking_reading_loop: read #{}: {} bytes, total: {} bytes",
            read_count,
            n,
            total_bytes
        );

        if n == 0 {
            tracing::trace!(
                "run_blocking_reading_loop: EOF reached after {} reads, {} total bytes",
                read_count,
                total_bytes
            );
            break;
        }

        let pkt = Packet {
            init_hash,
            init_bytes: if init_hash != 0 {
                // Only include init bytes for the first packet with a non-zero hash
                // Subsequent packets should have empty init_bytes
                let _bytes = init_bytes.clone();
                // Clear init_bytes for future packets
                Bytes::new()
            } else {
                Bytes::new()
            },
            media_bytes: Bytes::copy_from_slice(&buf[..n]),
            variant_index,
        };

        // Use synchronous send in blocking thread
        if sync_out.send(pkt).is_err() {
            // Consumer dropped.
            break;
        }
    }

    Ok(())
}

/// Create an IO error with the Other kind.
pub fn io_other(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg.to_string())
}

/// Trait for stream creation that can be used by packet producers.
///
/// This trait abstracts the creation of StreamDownload instances for different
/// source types (HTTP, HLS, etc.).
#[async_trait::async_trait]
pub trait StreamCreator {
    /// The source stream type.
    type SourceStream: stream_download::source::SourceStream;

    /// The parameters needed to create the source stream.
    type Params;

    /// Create a new StreamDownload instance.
    async fn create_stream_download(
        params: Self::Params,
    ) -> io::Result<StreamDownload<TempStorageProvider>>;
}

/// Generic packet producer implementation using a StreamCreator.
///
/// This struct provides a generic implementation of PacketProducer that
/// works with any StreamCreator.
pub struct GenericPacketProducer<C: StreamCreator> {
    params: C::Params,
}

impl<C: StreamCreator> Clone for GenericPacketProducer<C>
where
    C::Params: Clone,
{
    fn clone(&self) -> Self {
        Self {
            params: self.params.clone(),
        }
    }
}

impl<C: StreamCreator> GenericPacketProducer<C> {
    /// Create a new generic packet producer.
    pub fn new(params: C::Params) -> Self {
        Self { params }
    }
}

#[async_trait::async_trait]
impl<C: StreamCreator> crate::backends::PacketProducer for GenericPacketProducer<C>
where
    C::Params: Clone + Send + Sync,
{
    async fn run(
        &mut self,
        out: AsyncSender<Packet>,
        cancel: Option<CancellationToken>,
    ) -> io::Result<()> {
        let reader = C::create_stream_download(self.params.clone()).await?;

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
