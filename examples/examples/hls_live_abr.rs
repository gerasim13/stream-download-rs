use std::io::{self, BufRead};
use std::thread;
use std::time::Duration;

use tokio::sync::mpsc;

use stream_download_hls::{
    AbrConfig, AbrController, DownloaderConfig, HlsConfig, HlsManager, HlsResult, MediaStream,
    ResourceDownloader, SelectionMode,
};
use tracing::{info, metadata::LevelFilter, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
enum UserCommand {
    Auto,
    Manual(usize),
    List,
    Quit,
}

fn print_usage() {
    eprintln!("hls_live_abr - Live HLS ABR demo with AUTO/MANUAL selection");
    eprintln!("Usage:");
    eprintln!("  hls_live_abr [URL] [--mode auto|manual] [--variant N] [--max N]");
    eprintln!();
    eprintln!("Interactive commands during playback:");
    eprintln!("  auto            - switch to AUTO mode (ABR)");
    eprintln!("  manual <index>  - switch to MANUAL mode and lock to variant index");
    eprintln!("  list            - print variants and current selection");
    eprintln!("  quit            - exit");
    eprintln!();
}

fn spawn_stdin_command_loop(tx: mpsc::UnboundedSender<UserCommand>) {
    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else { break };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let mut parts = trimmed.split_whitespace();
            let cmd = parts.next().unwrap_or_default().to_lowercase();
            let parsed = match cmd.as_str() {
                "auto" => Some(UserCommand::Auto),
                "manual" => {
                    if let Some(idx_str) = parts.next() {
                        if let Ok(idx) = idx_str.parse::<usize>() {
                            Some(UserCommand::Manual(idx))
                        } else {
                            eprintln!("Invalid manual index: {idx_str}");
                            None
                        }
                    } else {
                        eprintln!("Usage: manual <index>");
                        None
                    }
                }
                "list" => Some(UserCommand::List),
                "quit" | "exit" => Some(UserCommand::Quit),
                other => {
                    eprintln!("Unknown command: {other}");
                    eprintln!("Commands: auto | manual <index> | list | quit");
                    None
                }
            };
            if let Some(cmd) = parsed {
                let _ = tx.send(cmd);
            }
        }
    });
}

#[tokio::main]
async fn main() -> HlsResult<()> {
    // Logging setup
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::default().add_directive(LevelFilter::INFO.into()))
        .with_line_number(true)
        .with_file(true)
        .init();

    // Defaults
    let mut master_playlist_url =
        "https://streams.radiomast.io/ref-128k-mp3-stereo/hls.m3u8".to_string();
    let mut start_mode = "auto".to_string();
    let mut manual_index: Option<usize> = None;
    let mut max_segments: Option<u64> = None;

    // Simple arg parsing
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        return Ok(());
    }

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" if i + 1 < args.len() => {
                start_mode = args[i + 1].to_lowercase();
                i += 2;
            }
            "--variant" if i + 1 < args.len() => {
                if let Ok(idx) = args[i + 1].parse::<usize>() {
                    manual_index = Some(idx);
                } else {
                    eprintln!("Invalid --variant value: {}", args[i + 1]);
                    print_usage();
                    return Ok(());
                }
                i += 2;
            }
            "--max" if i + 1 < args.len() => {
                if let Ok(n) = args[i + 1].parse::<u64>() {
                    max_segments = Some(n);
                } else {
                    eprintln!("Invalid --max value: {}", args[i + 1]);
                    print_usage();
                    return Ok(());
                }
                i += 2;
            }
            other => {
                if other.starts_with("http://") || other.starts_with("https://") {
                    master_playlist_url = other.to_string();
                    i += 1;
                } else {
                    eprintln!("Unknown argument: {}", other);
                    print_usage();
                    return Ok(());
                }
            }
        }
    }

    info!("Starting HLS live ABR demo");
    info!("  URL: {}", master_playlist_url);
    info!("  Mode: {}", start_mode);
    if let Some(idx) = manual_index {
        info!("  Manual variant index: {}", idx);
    }
    if let Some(max) = max_segments {
        info!("  Max segments: {}", max);
    } else {
        info!("  Max segments: unlimited (Ctrl+C to stop)");
    }

    // Build components
    let downloader = ResourceDownloader::new(DownloaderConfig::default());
    let hls_config = HlsConfig {
        // For live: None uses target_duration; Some(d) enforces fixed cadence
        live_refresh_interval: None,
        ..Default::default()
    };
    let manager = HlsManager::new(master_playlist_url.clone(), hls_config, downloader);

    let abr_config = AbrConfig::default();
    let initial_variant_index = 0usize;
    let initial_bandwidth = 2_000_000.0; // 2 Mbps initial guess
    let mut stream = AbrController::new(
        manager,
        abr_config,
        initial_variant_index,
        initial_bandwidth,
    );

    // Init (loads master and selects initial variant)
    info!("Initializing stream...");
    if let Err(e) = stream.init().await {
        warn!("Failed to initialize: {e}");
        // Continue: variants() might still be available if load partially succeeded
    }

    // Print variants
    let variants = stream.variants();
    if variants.is_empty() {
        warn!("No variants found; exiting.");
        return Ok(());
    }
    info!("Found {} variant(s):", variants.len());
    for (i, v) in variants.iter().enumerate() {
        info!(
            "  [{}] id={}, bandwidth={:?}, name={:?}, codecs={}",
            i,
            v.id.0,
            v.bandwidth,
            v.name,
            v.codec
                .as_ref()
                .and_then(|c| c.codecs.as_deref())
                .unwrap_or("N/A")
        );
    }

    // Apply initial mode selection
    match start_mode.as_str() {
        "manual" => {
            let idx = manual_index.unwrap_or(0).min(variants.len() - 1);
            let id = variants[idx].id;
            info!(
                "Switching to MANUAL mode with variant index {} (id={})",
                idx, id.0
            );
            if let Err(e) = stream.set_manual(id).await {
                warn!("Failed to switch to manual variant: {e}");
            }
        }
        "auto" | _ => {
            info!("Using AUTO mode (ABR)");
            stream.set_auto();
        }
    }

    // Command input channel
    let (tx, mut rx) = mpsc::unbounded_channel::<UserCommand>();
    spawn_stdin_command_loop(tx);

    // Probe loop with interactive control
    info!("Entering playback loop. Type 'list', 'auto', 'manual <index>' or 'quit'...");
    let mut count: u64 = 0;

    loop {
        tokio::select! {
            maybe_cmd = rx.recv() => {
                if let Some(cmd) = maybe_cmd {
                    match cmd {
                        UserCommand::Auto => {
                            stream.set_auto();
                            info!("Switched to AUTO mode (ABR)");
                        }
                        UserCommand::Manual(idx) => {
                            let id_opt = stream.variants().get(idx).map(|v| v.id);
                            if let Some(id) = id_opt {
                                match stream.set_manual(id).await {
                                    Ok(()) => info!("Switched to MANUAL mode: index {} (id={})", idx, id.0),
                                    Err(e) => warn!("Failed to switch to manual variant: {e}"),
                                }
                            } else {
                                warn!("Variant index {} out of range (0..{})", idx, stream.variants().len().saturating_sub(1));
                            }
                        }
                        UserCommand::List => {
                            let current = stream.current_variant_id();
                            let mode = match stream.selection_mode() {
                                SelectionMode::Auto => "AUTO",
                                SelectionMode::Manual => "MANUAL",
                            };
                            info!("Mode: {}", mode);
                            info!("Variants (current marked with '*'):");
                            for (i, v) in stream.variants().iter().enumerate() {
                                let star = if Some(v.id) == current { "*" } else { " " };
                                info!(
                                    " {}[{}] id={}, bandwidth={:?}, name={:?}, codecs={}",
                                    star,
                                    i,
                                    v.id.0,
                                    v.bandwidth,
                                    v.name,
                                    v.codec.as_ref().and_then(|c| c.codecs.as_deref()).unwrap_or("N/A")
                                );
                            }
                        }
                        UserCommand::Quit => {
                            info!("Quit command received.");
                            break;
                        }
                    }
                } else {
                    // channel closed
                    break;
                }
            }
            seg_res = stream.next_segment() => {
                match seg_res {
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
                        if let Some(max) = max_segments {
                            if count >= max {
                                info!("Reached max segments: {}", max);
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        info!("Stream ended (ENDLIST).");
                        break;
                    }
                    Err(e) => {
                        warn!("Error fetching next segment: {e}");
                        // Optional: retry after small delay
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }

    info!("Exiting.");
    Ok(())
}
