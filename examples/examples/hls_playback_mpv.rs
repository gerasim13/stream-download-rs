use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Duration;

use reqwest::Url;
use stream_download_hls::{
    AbrConfig, AbrController, DownloaderConfig, HlsConfig, HlsManager, MediaStream,
    ResourceDownloader,
};
use tracing::{info, metadata::LevelFilter, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::default().add_directive(LevelFilter::INFO.into()))
        .with_line_number(true)
        .with_file(true)
        .init();

    // You can change this to any public HLS master playlist URL (TS or fMP4).
    // For demo purposes we keep a default value; you can also pass a URL as the first CLI argument.
    let master_playlist_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "https://stream.silvercomet.top/hls/master.m3u8".to_string());

    info!("Initializing HLS stream from: {master_playlist_url}");

    // 1) Build components
    let downloader = ResourceDownloader::new(DownloaderConfig::default());
    let hls_config = HlsConfig {
        // Optional: provide a key processor callback if your key server requires custom processing.
        // key_processor_cb: Some(Arc::new(Box::new(|raw: Bytes| {
        //     // Example no-op processor
        //     raw
        // }))),
        live_refresh_interval: Some(Duration::from_secs(5)),
        ..Default::default()
    };
    let manager = HlsManager::new(master_playlist_url.clone(), hls_config, downloader);

    // Wrap in ABR controller (AUTO mode)
    let abr_config = AbrConfig::default();
    let mut stream = AbrController::new(manager, abr_config, 0, 2_000_000.0);
    stream.set_auto();

    // 2) Init the stream via ABR (loads master and selects initial variant)
    if let Err(e) = stream.init().await {
        warn!("Stream initialization failed: {e}");
        return Ok(());
    }

    // Log variants
    let variants = stream.variants();
    if variants.is_empty() {
        warn!("No variants found in master playlist");
        return Ok(());
    }
    info!("Found {} variants", variants.len());
    for v in variants {
        info!("  - id={}, bandwidth={:?}", v.id.0, v.bandwidth);
    }

    // 3) Start mpv and pipe segments to its stdin.
    //
    // Notes:
    // - The "-" argument tells mpv to read from stdin.
    // - We hint TS demuxer explicitly; for fMP4 streams, mpv is usually smart enough,
    //   but you may need to adjust args if your source is not TS.
    // - You can add "--really-quiet" or "--msg-level=all=v" for different verbosity.
    let mut child = Command::new("mpv")
        .arg("--no-terminal")
        .arg("--force-window=yes")
        // let mpv auto-detect container format
        .arg("--cache=yes")
        .arg("--cache-secs=10")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn();

    let mut child = match child {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to spawn mpv process: {e}");
            return Ok(());
        }
    };

    let mut stdin = match child.stdin.take() {
        Some(s) => s,
        None => {
            warn!("Failed to capture mpv stdin");
            return Ok(());
        }
    };

    info!("Starting playback loop: piping segments to mpv stdin...");

    // Attempt to write init segment if present (for fMP4)
    let init_url_opt: Option<String> = {
        let inner = stream.inner_stream();
        if let Some(playlist) = inner.current_media_playlist() {
            if let Some(init) = &playlist.init_segment {
                // Resolve media playlist URL using the first variant URI
                let variants = stream.variants();
                if let Some(v) = variants.get(0) {
                    let variant_uri = &v.uri;
                    let media_url = if variant_uri.starts_with("http") {
                        variant_uri.clone()
                    } else {
                        Url::parse(&master_playlist_url)
                            .and_then(|u| u.join(variant_uri))
                            .map(|u| u.to_string())
                            .unwrap_or_else(|_| variant_uri.clone())
                    };
                    let init_url = if init.uri.starts_with("http") {
                        init.uri.clone()
                    } else {
                        Url::parse(&media_url)
                            .and_then(|u| u.join(&init.uri))
                            .map(|u| u.to_string())
                            .unwrap_or_else(|_| init.uri.clone())
                    };
                    Some(init_url)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    };
    if let Some(init_url) = init_url_opt {
        match stream
            .inner_stream()
            .downloader()
            .download_bytes(&init_url, None)
            .await
        {
            Ok(bytes) => {
                info!("Writing init segment ({} bytes)", bytes.len());
                if let Err(e) = stdin.write_all(&bytes) {
                    warn!("Failed to write init segment to mpv stdin: {e}");
                }
                let _ = stdin.flush();
            }
            Err(e) => {
                warn!("Failed to download init segment: {e}");
            }
        }
    }

    // 4) Main loop: fetch segments and write them to mpv's stdin.
    let mut seg_count: u64 = 0;
    loop {
        match stream.next_segment().await {
            Ok(Some(seg)) => {
                seg_count += 1;
                let len = seg.data.len();
                info!(
                    "Segment #{} -> seq={}, variant={}, len={} bytes, duration={:?}{}",
                    seg_count,
                    seg.sequence,
                    seg.variant_id.0,
                    len,
                    seg.duration,
                    if seg.key.is_some() { ", encrypted" } else { "" }
                );

                if len == 0 {
                    warn!("Received empty segment; skipping write");
                    continue;
                }

                // Blocking write to mpv stdin (simple and reliable for an example)
                if let Err(e) = stdin.write_all(&seg.data) {
                    warn!("Failed to write segment to mpv stdin: {e}");
                    break;
                }
                // Flush after each write to lower latency
                if let Err(e) = stdin.flush() {
                    warn!("Failed to flush mpv stdin: {e}");
                    break;
                }
            }
            Ok(None) => {
                info!("Stream ended (no more segments). Closing mpv stdin...");
                // Close mpv stdin so it can terminate gracefully
                drop(stdin);
                break;
            }
            Err(e) => {
                warn!("Error fetching next segment: {e}");
                // Depending on your needs, you may want to retry or wait here.
                break;
            }
        }
    }

    // 5) Wait for mpv to exit
    match child.wait() {
        Ok(status) => info!("mpv exited with status: {status}"),
        Err(e) => warn!("Failed to wait for mpv process: {e}"),
    }

    info!("Playback example finished.");
    Ok(())
}
