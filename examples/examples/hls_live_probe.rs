use std::time::Duration;

use stream_download_hls::{
    DownloaderConfig, HlsConfig, HlsManager, HlsResult, MediaStream, ResourceDownloader,
};
use tracing::{info, metadata::LevelFilter, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> HlsResult<()> {
    // Logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::default().add_directive(LevelFilter::INFO.into()))
        .with_line_number(true)
        .with_file(true)
        .init();

    // Args:
    // 1) HLS master playlist URL (defaults to provided live radio stream)
    // 2) Optional number of segments to fetch (default: 10). Use 0 for unlimited.
    let master_playlist_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "https://streams.radiomast.io/ref-128k-mp3-stereo/hls.m3u8".to_string());
    let max_segments: u64 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10);

    info!("HLS live probe starting");
    info!("  URL: {}", master_playlist_url);
    if max_segments == 0 {
        info!("  Mode: unlimited (press Ctrl+C to stop)");
    } else {
        info!("  Mode: fetch up to {} segments", max_segments);
    }

    // Build components
    let downloader = ResourceDownloader::new(DownloaderConfig::default());
    let hls_config = HlsConfig {
        // For live streams, you can:
        // - set None to use playlist target_duration refresh
        // - set Some(Duration) to force a fixed refresh cadence
        live_refresh_interval: None,
        ..Default::default()
    };
    let mut manager = HlsManager::new(master_playlist_url.clone(), hls_config, downloader);

    // Init stream (loads master)
    if let Err(e) = manager.init().await {
        warn!("Failed to initialize HLS stream: {e}");
        return Ok(());
    }

    // Show variants
    let variants = manager.variants().to_vec();
    info!("Found {} variant(s):", variants.len());
    for v in &variants {
        info!(
            "  - id={}, bandwidth={:?}, name={:?}, codecs={}",
            v.id.0,
            v.bandwidth,
            v.name,
            v.codec
                .as_ref()
                .and_then(|c| c.codecs.as_deref())
                .unwrap_or("N/A")
        );
    }

    // Select the first variant by index (inherent method takes usize)
    if variants.is_empty() {
        warn!("No variants found in master playlist.");
        return Ok(());
    }
    if let Err(e) = manager.select_variant(0).await {
        warn!("Failed to select initial variant: {e}");
        return Ok(());
    }
    info!("Selected variant #0 (id={})", variants[0].id.0);

    // Probe loop
    info!("Starting live probe loop...");
    let mut count: u64 = 0;

    loop {
        match manager.next_segment().await {
            Ok(Some(seg)) => {
                count += 1;
                let encrypted = seg.key.is_some();
                info!(
                    "#{}: seq={}, variant={}, size={} bytes, duration={:?}{}",
                    count,
                    seg.sequence,
                    seg.variant_id.0,
                    seg.data.len(),
                    seg.duration,
                    if encrypted { ", encrypted" } else { "" }
                );

                if max_segments != 0 && count >= max_segments {
                    info!("Reached requested segment limit ({max_segments}). Stopping.");
                    break;
                }
            }
            Ok(None) => {
                // For VOD, this means end of stream. For live, this shouldn't normally happen
                // unless the server signaled ENDLIST.
                info!("Stream ended (ENDLIST).");
                break;
            }
            Err(e) => {
                warn!("Error fetching next segment: {e}");
                // For a live probe you may choose to continue:
                // tokio::time::sleep(Duration::from_secs(1)).await;
                // continue;
                break;
            }
        }
    }

    info!("HLS live probe finished.");
    Ok(())
}
