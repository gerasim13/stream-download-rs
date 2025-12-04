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
    AbrConfig, AbrController,
    DownloaderConfig, ResourceDownloader,
    HlsConfig, HlsManager, HlsResult, MediaStream,
};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> HlsResult<()> {
    // Initialize a simple logger for tracing
    tracing_subscriber::fmt::init();

    let master_playlist_url = "https://stream.silvercomet.top/hls/master.m3u8";
    info!("Initializing HLS stream from: {}", master_playlist_url);

    // 1. Set up the components
    // The ResourceDownloader is a thin wrapper around stream-download.
    let downloader = ResourceDownloader::new(DownloaderConfig::default());
    // The HlsConfig holds basic configuration for the HLS stream.
    let hls_config = HlsConfig::default();
    // The HlsManager is the core component that implements the HLS logic.
    let manager = HlsManager::new(master_playlist_url, hls_config, downloader);

    // In a real application, you would likely use an AbrController to manage
    // variant switching. For this basic example, we will interact with the
    // manager directly, pretending to be a simple ABR.
    let abr_config = AbrConfig::default();
    // For this example, let's just pick the first variant.
    let initial_variant_index = 0;
    let mut abr_controller = AbrController::new(manager, abr_config, initial_variant_index);

    // The player will interact with the `MediaStream` trait, which abstracts
    // away the HLS-specific implementation details.
    let stream: &mut dyn MediaStream = abr_controller.manager_mut();

    // 2. Initialize the stream
    // This will fetch and parse the master playlist.
    info!("Initializing stream...");
    if let Err(e) = stream.init().await {
        warn!(
            "Stream initialization failed as expected (implementation is a stub): {}",
            e
        );
        // In a real scenario, we would abort here. For the sake of demonstrating
        // the API flow, we'll continue with placeholder data.
        // Once the implementation is ready, this warning and the following
        // return can be removed.
        return Ok(());
    }
    info!("Stream initialized successfully.");

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

    // 4. Select a variant to start playback
    // For this example, we'll just pick the first one.
    // An ABR controller would make a more intelligent choice based on bandwidth.
    if let Some(first_variant) = variants.first() {
        info!("Selecting variant #{} for playback", first_variant.id.0);
        stream.select_variant(first_variant.id).await?;
    } else {
        warn!("No variants found in the playlist.");
        return Ok(());
    }

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
