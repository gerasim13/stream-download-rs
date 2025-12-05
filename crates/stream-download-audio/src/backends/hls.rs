use std::io::{Read, Result as IoResult, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use kanal as kchan;
use symphonia::core::io::{MediaSource, MediaSourceStream, MediaSourceStreamOptions};
use tracing::{debug, error, info};

use stream_download_hls::{
    DownloaderConfig, HlsConfig, HlsManager, MediaStream, ResourceDownloader, VariantStream,
};

/// A blocking, non-seekable MediaSource that streams bytes coming from an HLS pipeline.
///
/// This type aggregates:
/// - an optional initialization segment (EXT-X-MAP) bytes,
/// - followed by a continuous stream of media segment bytes.
///
/// The bytes are produced asynchronously by a background task driving `HlsManager`.
/// The `Read` implementation blocks until enough data is available or until the stream ends.
pub struct HlsMediaSource {
    rx: kchan::Receiver<Vec<u8>>,
    cur: std::io::Cursor<Vec<u8>>,
    eof: bool,
}

impl HlsMediaSource {
    fn new(rx: kchan::Receiver<Vec<u8>>) -> Self {
        Self {
            rx,
            cur: std::io::Cursor::new(Vec::new()),
            eof: false,
        }
    }

    fn refill(&mut self) -> IoResult<()> {
        match self.rx.recv() {
            Ok(chunk) => {
                self.cur = std::io::Cursor::new(chunk);
                Ok(())
            }
            Err(_) => {
                // Producer dropped; mark EOF.
                self.eof = true;
                Ok(())
            }
        }
    }
}

impl Read for HlsMediaSource {
    fn read(&mut self, out: &mut [u8]) -> IoResult<usize> {
        if self.eof {
            return Ok(0);
        }

        // If current buffer depleted, attempt to refill from channel (blocking).
        if (self.cur.position() as usize) >= self.cur.get_ref().len() {
            self.refill()?;
            if self.eof {
                return Ok(0);
            }
        }

        self.cur.read(out)
    }
}

impl Seek for HlsMediaSource {
    fn seek(&mut self, _pos: SeekFrom) -> IoResult<u64> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "seek not supported for HLS stream",
        ))
    }
}

impl MediaSource for HlsMediaSource {
    fn is_seekable(&self) -> bool {
        // Live HLS is inherently non-seekable. For VOD, seek support can be added later
        // by implementing segment indexing and range re-fetch.
        false
    }

    fn byte_len(&self) -> Option<u64> {
        // Unknown total length for streaming HLS.
        None
    }
}

/// A controller for the background HLS producer driving the `HlsMediaSource`.
///
/// Dropping the controller will attempt to stop the producer thread gracefully.
pub struct HlsSourceController {
    stop_tx: kchan::Sender<()>,
    join: Option<JoinHandle<()>>,
}

impl HlsSourceController {
    /// Signal the background producer to stop and wait for it to finish.
    pub fn stop(mut self) {
        let _ = self.stop_tx.send(());
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

impl Drop for HlsSourceController {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(());
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// Open a blocking MediaSourceStream for HLS by streaming the init segment (if any) and subsequent media segments.
///
/// This function:
/// - Spawns a background thread with a Tokio runtime,
/// - Initializes `HlsManager`, selects a variant (either the lowest bandwidth or index 0 if unknown),
/// - Streams the init segment (if present),
/// - Then continuously fetches media segments and sends them to the `HlsMediaSource` via a channel.
///
/// Important notes:
/// - The returned `MediaSourceStream` is non-seekable.
/// - For live streams, this will run indefinitely until stopped through the controller or until EOF.
/// - For VOD streams, EOF is naturally reached when `#EXT-X-ENDLIST` and segments end.
pub fn open_hls_media_source(
    url: &str,
    hls_config: HlsConfig,
) -> IoResult<(MediaSourceStream, HlsSourceController)> {
    let (data_tx, data_rx) = kchan::unbounded::<Vec<u8>>();
    let (stop_tx, stop_rx) = kchan::unbounded::<()>();

    let url_str = url.to_string();

    // Spawn a background thread to drive the async HLS pipeline.
    let join = thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                error!("failed to build tokio runtime for HLS backend: {e}");
                let _ = data_tx.close();
                return;
            }
        };

        rt.block_on(async move {
            // Prepare downloader and manager.
            let downloader = ResourceDownloader::new(DownloaderConfig::default());
            let mut manager = HlsManager::new(url_str.clone(), hls_config, downloader);

            // Load master and select a variant.
            if let Err(e) = manager.load_master().await {
                error!("HLS: failed to load master playlist: {e:?}");
                let _ = data_tx.close();
                return;
            }

            let master = match manager.master() {
                Some(m) => m,
                None => {
                    error!("HLS: master playlist not available after load_master()");
                    let _ = data_tx.close();
                    return;
                }
            };

            let chosen_index = select_variant_index(master.variants.as_slice());
            info!(
                "HLS: selecting variant index {} of {}",
                chosen_index,
                master.variants.len()
            );

            if let Err(e) = manager.select_variant(chosen_index).await {
                error!("HLS: failed to select variant {chosen_index}: {e:?}");
                let _ = data_tx.close();
                return;
            }

            // Try to download and send init segment first (for fMP4).
            match manager.download_init_segment().await {
                Ok(Some(bytes)) => {
                    let _ = data_tx.send(bytes);
                }
                Ok(None) => {
                    debug!("HLS: no init segment present");
                }
                Err(e) => {
                    error!("HLS: failed to download init segment: {e:?}");
                    // Non-fatal: proceed with media segments.
                }
            }

            // Stream media segments until EOF or stop signal.
            loop {
                if stop_rx.try_recv().is_ok() {
                    info!("HLS: stop signal received, terminating producer");
                    break;
                }

                match manager.next_segment().await {
                    Ok(Some(seg)) => {
                        // Convert Bytes to Vec<u8>
                        let chunk = seg.data.to_vec();
                        if data_tx.send(chunk).is_err() {
                            // Consumer dropped; stop.
                            break;
                        }
                    }
                    Ok(None) => {
                        // End of VOD playlist.
                        info!("HLS: end of stream");
                        break;
                    }
                    Err(e) => {
                        error!("HLS: next_segment error: {e:?}");
                        // Decide whether to continue or break; for now, break on errors.
                        break;
                    }
                }
            }

            // Close the data channel so the MediaSource sees EOF.
            let _ = data_tx.close();
        });
    });

    let source = HlsMediaSource::new(data_rx);
    let mss = MediaSourceStream::new(
        Box::new(source) as Box<dyn MediaSource>,
        MediaSourceStreamOptions::default(),
    );

    Ok((
        mss,
        HlsSourceController {
            stop_tx,
            join: Some(join),
        },
    ))
}

/// Variant selection heuristic:
/// - Prefer the variant with the lowest advertised bandwidth (safer startup),
/// - Fallback to index 0 if bandwidth is missing or list is empty.
fn select_variant_index(variants: &[VariantStream]) -> usize {
    if variants.is_empty() {
        return 0;
    }
    let mut best_idx = 0usize;
    let mut best_bw = u64::MAX;
    for (i, v) in variants.iter().enumerate() {
        if let Some(bw) = v.bandwidth {
            if bw < best_bw {
                best_bw = bw;
                best_idx = i;
            }
        }
    }
    // If no bandwidth was set on any variant, stick to 0.
    best_idx
}
