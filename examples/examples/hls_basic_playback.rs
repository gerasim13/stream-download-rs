//! A basic example for HLS playback using the stream-download-hls crate.
//!
//! This example demonstrates the intended high-level API usage for a player
//! or a similar consumer.
//!
//! It performs the following steps:
//! 1. Initializes an HLS stream from a master playlist URL.
//! 2. Prints the list of available variants (codecs, bitrates).
//! 3. Selects the first variant in the list.
//! 4. Fetches and prints metadata for the first few segments of the selected stream.
//!
//! NOTE: This example will not run successfully until the stub implementations
//! in `HlsManager` are replaced with real logic for parsing and downloading.
//! It serves as a "living specification" for the API design.

use std::time::Duration;
use stream_download_hls::{
    AbrConfig, AbrController, DownloaderConfig, HlsConfig, HlsManager, HlsResult, MediaStream,
    ResourceDownloader,
};
use tracing::{info, metadata::LevelFilter, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> HlsResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::default().add_directive(LevelFilter::INFO.into()))
        .with_line_number(true)
        .with_file(true)
        .init();

    let master_playlist_url = "https://stream.silvercomet.top/hls/master.m3u8";
    info!("Initializing HLS stream from: {}", master_playlist_url);

    // 1. Set up the components
    // The ResourceDownloader is a thin wrapper around stream-download.
    let downloader = ResourceDownloader::new(DownloaderConfig::default());
    // The HlsConfig holds basic configuration for the HLS stream.
    let hls_config = HlsConfig::default();
    // The HlsManager is the core component that implements the HLS logic.
    let manager = HlsManager::new(master_playlist_url, hls_config, downloader);

    // In a real application, you would wrap the HlsManager in an AbrController
    // to enable automatic variant switching.
    let abr_config = AbrConfig::default();
    // For this example, we'll start with the first variant found.
    let initial_variant_index = 0;
    let initial_bandwidth = 1_000_000.0; // 1 Mbps
    let abr_controller = AbrController::new(
        manager,
        abr_config,
        initial_variant_index,
        initial_bandwidth,
    );

    // The player interacts with the `MediaStream` trait. `AbrController` also
    // implements `MediaStream`, adding its logic on top of the underlying stream.
    // We use a Box to hold the trait object.
    let mut stream: Box<dyn MediaStream> = Box::new(abr_controller);

    // 2. Initialize the stream
    // For AbrController, `init()` will initialize the underlying stream *and*
    // select the initial variant.
    info!("Initializing stream...");
    match stream.init().await {
        Ok(()) => info!("Stream initialized successfully."),
        Err(e) => {
            // Don't exit early — continue to show variants and attempt fetching segments
            // so the example always prints something useful.
            warn!("Stream initialization failed: {}", e);
        }
    }

    // 3. Inspect available variants
    let variants = stream.variants();
    info!("Found {} variants:", variants.len());
    for variant in variants {
        let bandwidth = variant.bandwidth.unwrap_or(0);
        let codecs = variant
            .codec
            .as_ref()
            .and_then(|c| c.codecs.as_deref())
            .unwrap_or("N/A");
        info!(
            "  - Variant #{}: Bandwidth={}, Codecs='{}'",
            variant.id.0, bandwidth, codecs
        );
    }

    // 4. Initial variant selection
    // This step is no longer needed here, as the AbrController's `init` method
    // has already selected the initial variant for us.
    info!(
        "Initial variant #{} was selected by the AbrController during init.",
        initial_variant_index
    );

    // 5. Fetch the first few segments
    info!("Fetching first 5 segments...");
    for i in 0..5 {
        info!("Waiting for segment #{}...", i);
        match stream.next_segment().await {
            Ok(Some(segment_data)) => {
                info!(
                    "  - Got Segment: Seq={}, VariantID={}, Size={} bytes, Duration={:?}",
                    segment_data.sequence,
                    segment_data.variant_id.0,
                    segment_data.data.len(),
                    segment_data.duration,
                );
                if let Some(key) = &segment_data.key {
                    info!("    - Encrypted with: {:?}", key.method);
                }
            }
            Ok(None) => {
                info!("Stream ended.");
                break;
            }
            Err(e) => {
                warn!("Error fetching next segment: {}", e);
                // In a real player, you might retry or handle the error
                break;
            }
        }
    }

    info!("Example finished.");
    Ok(())
}
