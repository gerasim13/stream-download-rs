use tokio_util::sync::CancellationToken;
use tracing::{error, trace};

use stream_download_hls::{
    AbrConfig, AbrController, Bytes, DownloaderConfig, HlsConfig, HlsManager, MediaStream,
    ResourceDownloader, SelectionMode, VariantStream,
};

use crate::pipeline::Packet;
use async_ringbuf::AsyncHeapProd;
use async_ringbuf::traits::producer::AsyncProducer;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

/// Pick a reasonable default variant index from the master playlist.
/// Prefer audio-capable variants; otherwise pick the lowest bandwidth.
fn select_variant_index(variants: &[VariantStream]) -> usize {
    if variants.is_empty() {
        return 0;
    }
    // Prefer entries with audio codec in CODECS, else fallback to lowest bandwidth.
    let mut best_idx = 0usize;
    let mut best_score: i32 = i32::MIN;
    for (i, v) in variants.iter().enumerate() {
        let mut score = 0;
        if let Some(c) = &v.codec {
            if let Some(codecs) = &c.codecs {
                let s = codecs.to_ascii_lowercase();
                if s.contains("mp4a")
                    || s.contains("aac")
                    || s.contains("flac")
                    || s.contains("opus")
                {
                    score += 10;
                }
            }
        }
        if let Some(bw) = v.bandwidth {
            // Prefer lower bandwidth slightly if scores are equal.
            score += (1_000_000_000u64.saturating_sub(bw) / 1_000_000) as i32;
        }
        if score > best_score {
            best_score = score;
            best_idx = i;
        }
    }
    best_idx
}

/// A blocking, non-seekable MediaSource that streams bytes coming from an HLS pipeline.
///
/// This type aggregates:
/// - an optional initialization segment (EXT-X-MAP) bytes,
/// - followed by a continuous stream of media segment bytes.
///
/// The bytes are produced asynchronously by a background task driving `HlsManager`.
/// The `Read` implementation blocks until enough data is available or until the stream ends.
/// Spawn an HLS packet producer that writes `Packet` into an async ring for the decoder.
/// Returns a `CancellationToken` to stop the producer.
pub async fn run_hls_packet_producer(
    url: String,
    hls_config: HlsConfig,
    abr_config: AbrConfig,
    selection: SelectionMode,
    mut out: AsyncHeapProd<Packet>,
    cancel: CancellationToken,
) {
    let downloader = ResourceDownloader::new(DownloaderConfig::default());
    let mut manager = HlsManager::new(url.clone(), hls_config, downloader);
    let cancel_child = cancel.child_token();

    if let Err(e) = manager.load_master().await {
        error!("HLS(packet): failed to load master: {e:?}");
        return;
    }
    let master = match manager.master() {
        Some(m) => m,
        None => {
            error!("HLS(packet): master not available after load_master()");
            return;
        }
    };

    // Choose initial variant.
    let chosen_index = match selection {
        SelectionMode::Auto => select_variant_index(master.variants.as_slice()),
        SelectionMode::Manual(vid) => {
            // Find the index for the requested VariantId; if not found, fallback.
            if let Some(pos) = master.variants.iter().position(|v| v.id == vid) {
                pos
            } else {
                select_variant_index(master.variants.as_slice())
            }
        }
    };
    let init_bw = master.variants[chosen_index].bandwidth.unwrap_or(0) as f64;

    let mut controller = AbrController::new(
        manager,
        abr_config.clone(),
        selection,
        chosen_index,
        init_bw,
    );
    if let Err(e) = controller.init().await {
        error!("HLS(packet): controller init failed: {e:?}");
        return;
    }

    // Compute initial init segment and its hash (if present).
    let mut last_init_hash: u64;
    match controller.inner_stream_mut().download_init_segment().await {
        Ok(Some(init_vec)) => {
            let init_bytes = Bytes::from(init_vec);
            let mut hasher = DefaultHasher::new();
            hasher.write(init_bytes.as_ref());
            last_init_hash = hasher.finish();
            // Don't send a packet yet; wait for first media to bundle.
            let mut last_variant_id = controller.current_variant_id();

            loop {
                if cancel_child.is_cancelled() {
                    trace!("HLS(packet): cancel requested, stopping producer");
                    break;
                }
                match controller.next_segment().await {
                    Ok(Some(seg)) => {
                        let mut init_hash = last_init_hash;
                        let mut init_bytes: Bytes = Bytes::new();
                        if Some(seg.variant_id) != last_variant_id {
                            // Variant switched: fetch new init.
                            match controller.inner_stream_mut().download_init_segment().await {
                                Ok(Some(new_init)) => {
                                    let mut hasher = DefaultHasher::new();
                                    hasher.write(new_init.as_ref());
                                    init_hash = hasher.finish();
                                    last_init_hash = init_hash;
                                    init_bytes = Bytes::from(new_init);
                                }
                                Ok(None) => {
                                    // No init provided by the stream; keep previous hash and empty init.
                                    trace!("HLS(packet): switched variant has no init segment");
                                }
                                Err(e) => {
                                    error!("HLS(packet): failed to download switched init: {e:?}");
                                }
                            }
                            last_variant_id = Some(seg.variant_id);
                        } else {
                            // First packet after init fetch: attach init to the first media packet.
                            if init_bytes.is_empty() && last_init_hash != 0 {
                                // Re-fetch init to bundle with this packet (ensure decoder can probe).
                                match controller.inner_stream_mut().download_init_segment().await {
                                    Ok(Some(b)) => init_bytes = Bytes::from(b),
                                    _ => {}
                                }
                            }
                        }

                        let variant_idx = seg.variant_id.0;
                        let pkt = Packet {
                            init_hash,
                            init_bytes,
                            media_bytes: seg.data,
                            variant_index: Some(variant_idx),
                        };
                        if out.push(pkt).await.is_err() {
                            trace!("HLS(packet): consumer dropped, stopping");
                            break;
                        }
                    }
                    Ok(None) => {
                        trace!("HLS(packet): end of stream (VOD)");
                        break;
                    }
                    Err(e) => {
                        trace!("HLS(packet): next_segment error: {e:?}");
                        break;
                    }
                }
            }
        }
        Ok(None) => {
            // No init segment; proceed sending media packets with empty init.
            let mut last_variant_id = controller.current_variant_id();
            loop {
                if cancel_child.is_cancelled() {
                    trace!("HLS(packet): cancel requested, stopping producer");
                    break;
                }
                match controller.next_segment().await {
                    Ok(Some(seg)) => {
                        if Some(seg.variant_id) != last_variant_id {
                            last_variant_id = Some(seg.variant_id);
                        }
                        let variant_idx = seg.variant_id.0;
                        let pkt = Packet {
                            init_hash: 0,
                            init_bytes: Bytes::new(),
                            media_bytes: seg.data,
                            variant_index: Some(variant_idx),
                        };
                        if out.push(pkt).await.is_err() {
                            trace!("HLS(packet): consumer dropped, stopping");
                            break;
                        }
                    }
                    Ok(None) => {
                        trace!("HLS(packet): end of stream (VOD)");
                        break;
                    }
                    Err(e) => {
                        trace!("HLS(packet): next_segment error: {e:?}");
                        break;
                    }
                }
            }
        }
        Err(e) => {
            error!("HLS(packet): failed to download init segment: {e:?}");
        }
    }
}
