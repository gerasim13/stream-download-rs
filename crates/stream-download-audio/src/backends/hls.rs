use std::io::{Read, Result as IoResult, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, Ordering};
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
    // Virtual absolute position from the beginning of the concatenated stream.
    pos: u64,
    // Absolute position at the start of the current `cur` buffer.
    base_pos: u64,
    // Seek gate flag to control when seeking is allowed (disabled during probe).
    seek_enabled: Arc<AtomicBool>,
}

impl HlsMediaSource {
    fn new(rx: kchan::Receiver<Vec<u8>>, seek_enabled: Arc<AtomicBool>) -> Self {
        Self {
            rx,
            cur: std::io::Cursor::new(Vec::new()),
            eof: false,
            pos: 0,
            base_pos: 0,
            seek_enabled,
        }
    }

    fn refill(&mut self) -> IoResult<()> {
        match self.rx.recv() {
            Ok(chunk) => {
                // New chunk begins at the current absolute position.
                self.base_pos = self.pos;
                let len = chunk.len();
                debug!(
                    "HLSMediaSource::refill: received chunk bytes={} base_pos={} pos={}",
                    len, self.base_pos, self.pos
                );
                self.cur = std::io::Cursor::new(chunk);
                Ok(())
            }
            Err(_) => {
                // Producer dropped; mark EOF.
                self.eof = true;
                debug!(
                    "HLSMediaSource::refill: channel closed -> EOF at pos={} base_pos={}",
                    self.pos, self.base_pos
                );
                Ok(())
            }
        }
    }
    /// Forward-only seek to an absolute target position.
    ///
    /// - If target is ahead of the current position, this will read/discard bytes
    ///   from the current and subsequent chunks until reaching `target`, blocking
    ///   for new chunks as needed.
    /// - If target falls within the current chunk (including backwards within it),
    ///   this will adjust the cursor within `cur`.
    /// - Seeking backwards outside the current chunk, or seeking relative to End, is unsupported.
    fn seek_to_forward_only(&mut self, target: u64) -> IoResult<u64> {
        if target == self.pos {
            return Ok(self.pos);
        }

        // If target lies within the current buffer (even if it's behind),
        // adjust the local cursor position.
        let cur_len = self.cur.get_ref().len() as u64;
        let cur_off = self.cur.position() as u64;
        if target >= self.base_pos && target <= self.base_pos + cur_len {
            let new_off = target - self.base_pos;
            self.cur.set_position(new_off);
            self.pos = self.base_pos + new_off;
            return Ok(self.pos);
        }

        // If target is behind and not within current buffer, we cannot seek.
        if target < self.pos {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "backward seek not supported for HLS stream",
            ));
        }

        // Otherwise, target is ahead; discard until we reach it.
        loop {
            // Consume remaining bytes in current buffer first.
            let cur_len = self.cur.get_ref().len() as u64;
            let cur_off = self.cur.position() as u64;

            if self.pos < target {
                if cur_off < cur_len {
                    let rem_in_buf = cur_len - cur_off;
                    let need = target - self.pos;
                    let step = rem_in_buf.min(need) as u64;
                    // Advance within buffer.
                    self.cur.set_position(cur_off + step);
                    self.pos += step;
                    if self.pos == target {
                        return Ok(self.pos);
                    }
                } else {
                    // Need next chunk.
                    self.refill()?;
                    if self.eof {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "cannot seek forward: stream ended",
                        ));
                    }
                }
            } else {
                return Ok(self.pos);
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

        let want = out.len();
        let n = self.cur.read(out)?;
        self.pos = self.base_pos + self.cur.position() as u64;
        debug!(
            "HLSMediaSource::read: requested={} returned={} pos={} base_pos={} cur_pos={}",
            want,
            n,
            self.pos,
            self.base_pos,
            self.cur.position()
        );
        Ok(n)
    }
}

impl Seek for HlsMediaSource {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        // Gate seeks until decoder is initialized to avoid tail-probing during probe.
        if !self.seek_enabled.load(Ordering::Relaxed) {
            debug!("HLSMediaSource::seek: denied during probe: {:?}", pos);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "seek disabled during probe",
            ));
        }
        match pos {
            SeekFrom::Start(n) => {
                let res = self.seek_to_forward_only(n);
                match &res {
                    Ok(np) => debug!("HLSMediaSource::seek(Start): target={} -> pos={}", n, np),
                    Err(e) => debug!("HLSMediaSource::seek(Start): target={} -> error={}", n, e),
                }
                res
            }
            SeekFrom::Current(off) => {
                if off >= 0 {
                    let target = self.pos.saturating_add(off as u64);
                    let res = self.seek_to_forward_only(target);
                    match &res {
                        Ok(np) => debug!(
                            "HLSMediaSource::seek(Current+): off={} target={} -> pos={}",
                            off, target, np
                        ),
                        Err(e) => debug!(
                            "HLSMediaSource::seek(Current+): off={} target={} -> error={}",
                            off, target, e
                        ),
                    }
                    res
                } else {
                    // Attempt a limited backward seek within the current buffer.
                    let back = (-off) as u64;
                    if back <= self.cur.position() as u64 {
                        let new_off = self.cur.position() as u64 - back;
                        self.cur.set_position(new_off);
                        self.pos = self.base_pos + new_off;
                        debug!(
                            "HLSMediaSource::seek(Current- within chunk): back={} -> pos={}",
                            back, self.pos
                        );
                        Ok(self.pos)
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Unsupported,
                            "backward seek outside current chunk is not supported",
                        ))
                    }
                }
            }
            SeekFrom::End(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "seek from end is not supported (unknown length)",
            )),
        }
    }
}

impl MediaSource for HlsMediaSource {
    fn is_seekable(&self) -> bool {
        // Seekability is gated: disabled during probe; can be enabled after decoder init.
        self.seek_enabled.load(Ordering::Relaxed)
    }

    fn byte_len(&self) -> Option<u64> {
        // Unknown total length for streaming HLS.
        None
    }
}

/// A controller for the background HLS producer driving the `HlsMediaSource`.
///
/// Dropping the controller is a no-op; the producer keeps running until EOF or `stop()` is called.
pub struct HlsSourceController {
    stop_tx: kchan::Sender<()>,
    join: Option<JoinHandle<()>>,
    seek_flag: Arc<AtomicBool>,
}

impl HlsSourceController {
    /// Enable seek on the underlying `HlsMediaSource` (typically after probe/decoder init).
    pub fn enable_seek(&self) {
        self.seek_flag.store(true, Ordering::Relaxed);
    }

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
        // no-op: avoid premature stop; the producer will end on EOF or explicit stop()
    }
}

/// Open a blocking MediaSourceStream for HLS by streaming the init segment (if any) and subsequent media segments.
///
/// This function:
/// - Spawns a background thread with a Tokio runtime,
/// - Initializes `HlsManager`, selects a variant (either the provided index or the lowest bandwidth),
/// - Streams the init segment (if present),
/// - Then continuously fetches media segments and sends them to the `HlsMediaSource` via a channel.
///
/// Important notes:
/// - The returned `MediaSourceStream` is non-seekable.
/// - For live streams, this will run indefinitely until stopped through the controller or until EOF.
/// - For VOD streams, EOF is naturally reached when `#EXT-X-ENDLIST` and segments end.
///
/// Parameters:
/// - `initial_variant_index`: Optional index into the master playlist variants to select initially.
///    If `None` or out of bounds, the lowest bandwidth variant is selected.
pub fn open_hls_media_source(
    url: &str,
    hls_config: HlsConfig,
    initial_variant_index: Option<usize>,
) -> IoResult<(MediaSourceStream, HlsSourceController)> {
    let (data_tx, data_rx) = kchan::bounded::<Vec<u8>>(8);
    let (stop_tx, stop_rx) = kchan::unbounded::<()>();

    let url_str = url.to_string();

    // Spawn a background thread to drive the async HLS pipeline.
    let join = thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                error!("failed to build tokio runtime for HLS backend: {e}");
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
                return;
            }

            let master = match manager.master() {
                Some(m) => m,
                None => {
                    error!("HLS: master playlist not available after load_master()");
                    return;
                }
            };

            let chosen_index = initial_variant_index
                .filter(|&i| i < master.variants.len())
                .unwrap_or_else(|| select_variant_index(master.variants.as_slice()));
            info!(
                "HLS: selecting variant index {} of {} (initial={:?})",
                chosen_index,
                master.variants.len(),
                initial_variant_index
            );

            if let Err(e) = manager.select_variant(chosen_index).await {
                error!("HLS: failed to select variant {chosen_index}: {e:?}");
                return;
            }

            // Try to download and send init segment first (for fMP4).
            match manager.download_init_segment().await {
                Ok(Some(bytes)) => {
                    // Some CMAF init segments include a top-level 'sidx' box which makes the demuxer
                    // assume segmented mode and try random seeks. Strip 'sidx' to keep linear playback.
                    fn strip_sidx(data: &[u8]) -> Vec<u8> {
                        let mut out = Vec::with_capacity(data.len());
                        let mut pos = 0usize;
                        while pos + 8 <= data.len() {
                            let size = u32::from_be_bytes([
                                data[pos],
                                data[pos + 1],
                                data[pos + 2],
                                data[pos + 3],
                            ]) as u64;
                            let typ = [data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7]];
                            let (hdr_len, box_size) = if size == 1 {
                                if pos + 16 > data.len() {
                                    break;
                                }
                                let larg = u64::from_be_bytes([
                                    data[pos + 8],
                                    data[pos + 9],
                                    data[pos + 10],
                                    data[pos + 11],
                                    data[pos + 12],
                                    data[pos + 13],
                                    data[pos + 14],
                                    data[pos + 15],
                                ]);
                                (16usize, larg as usize)
                            } else if size == 0 {
                                // box extends to end of buffer
                                (8usize, data.len() - pos)
                            } else {
                                (8usize, size as usize)
                            };
                            if box_size < hdr_len || pos + box_size > data.len() {
                                // malformed size; copy the rest and stop
                                out.extend_from_slice(&data[pos..]);
                                pos = data.len();
                                break;
                            }
                            if &typ == b"sidx" {
                                // skip this box entirely
                                pos += box_size;
                                continue;
                            } else {
                                out.extend_from_slice(&data[pos..pos + box_size]);
                                pos += box_size;
                            }
                        }
                        if pos < data.len() {
                            out.extend_from_slice(&data[pos..]);
                        }
                        out
                    }
                    let cleaned = strip_sidx(&bytes);
                    let _ = data_tx.send(cleaned);
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
                // stop check disabled: do not terminate producer on spurious stop signals

                match manager.next_segment().await {
                    Ok(Some(seg)) => {
                        // Convert Bytes to Vec<u8>
                        let chunk = seg.data.to_vec();
                        debug!(
                            "HLS: sending segment seq={} bytes={}",
                            seg.sequence,
                            chunk.len()
                        );
                        if data_tx.send(chunk).is_err() {
                            // Consumer dropped; stop.
                            break;
                        }
                    }
                    Ok(None) => {
                        // End of VOD playlist.
                        debug!("HLS: end of stream reached (playlist ENDLIST or no more segments)");
                        break;
                    }
                    Err(e) => {
                        error!("HLS: next_segment error: {e:?}");
                        // Decide whether to continue or break; for now, break on errors.
                        break;
                    }
                }
            }
        });
    });

    // Initialize seek gate disabled; playback code can enable after decoder init.
    let seek_flag = Arc::new(AtomicBool::new(false));

    let source = HlsMediaSource::new(data_rx, seek_flag.clone());
    let mss = MediaSourceStream::new(
        Box::new(source) as Box<dyn MediaSource>,
        MediaSourceStreamOptions::default(),
    );

    Ok((
        mss,
        HlsSourceController {
            stop_tx,
            join: Some(join),
            seek_flag,
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
