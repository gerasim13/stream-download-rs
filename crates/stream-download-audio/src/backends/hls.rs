use std::io::{Read, Result as IoResult, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use tokio_util::sync::CancellationToken;

use kanal as kchan;
use symphonia::core::io::{MediaSource, MediaSourceStream, MediaSourceStreamOptions};
use tracing::{debug, error, info, trace};

use stream_download_hls::{
    AbrConfig, AbrController, DownloaderConfig, HlsConfig, HlsManager, MediaStream,
    ResourceDownloader, SelectionMode, VariantId, VariantStream,
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
                trace!(
                    "HLSMediaSource::refill: received chunk bytes={} base_pos={} pos={}",
                    len, self.base_pos, self.pos
                );
                self.cur = std::io::Cursor::new(chunk);
                Ok(())
            }
            Err(_) => {
                // Producer dropped; mark EOF.
                self.eof = true;
                trace!(
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

        // If current buffer depleted, attempt to refill with bounded waits to let producer progress.
        while (self.cur.position() as usize) >= self.cur.get_ref().len() && !self.eof {
            self.refill()?;
            if (self.cur.position() as usize) < self.cur.get_ref().len() || self.eof {
                break;
            }
            // Briefly yield to avoid busy-wait and allow async producer to make progress.
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        if self.eof {
            return Ok(0);
        }

        let want = out.len();
        let n = self.cur.read(out)?;
        self.pos = self.base_pos + self.cur.position() as u64;
        trace!(
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
    cancel: CancellationToken,
    join: Mutex<Option<JoinHandle<()>>>,
    seek_flag: Arc<AtomicBool>,
}

impl HlsSourceController {
    /// Enable seek on the underlying `HlsMediaSource` (typically after probe/decoder init).
    pub fn enable_seek(&self) {
        self.seek_flag.store(true, Ordering::Relaxed);
    }

    /// Signal the background producer to stop and wait for it to finish.
    pub async fn stop(&self) {
        self.cancel.cancel();
        // Take the JoinHandle out under the mutex, then drop the guard
        // before awaiting to avoid holding a non-Send MutexGuard across .await.
        let join = {
            if let Ok(mut guard) = self.join.lock() {
                guard.take()
            } else {
                None
            }
        };
        if let Some(j) = join {
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
pub async fn open_hls_media_source_async(
    url: String,
    hls_config: HlsConfig,
    abr_config: AbrConfig,
    initial_mode: SelectionMode,
    initial_variant_index: Option<usize>,
) -> IoResult<(MediaSourceStream<'static>, HlsSourceController)> {
    let (data_tx, data_rx) = kchan::bounded::<Vec<u8>>(8);

    let url_str = url.clone();
    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();

    trace!("HLS: preparing worker spawn for URL: {}", url_str);

    // Spawn a background task to drive the async HLS pipeline.
    let join: JoinHandle<()> = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("HLS runtime build failed");
        rt.block_on(async move {
        // Prepare downloader and manager and wrap with ABR controller.
        let downloader = ResourceDownloader::new(DownloaderConfig::default());
        let mut manager = HlsManager::new(url_str.clone(), hls_config, downloader);

        // Load master and select a starting variant index (for initial bandwidth and logging).
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

        // Log master variants for troubleshooting.
        for (i, v) in master.variants.iter().enumerate() {
            let bw = v
                .bandwidth
                .map(|b| b.to_string())
                .unwrap_or_else(|| "unknown".into());
            let name = v.name.as_deref().unwrap_or("-");
            let codecs = v
                .codec
                .as_ref()
                .and_then(|c| c.codecs.clone())
                .unwrap_or_else(|| "-".into());
            trace!(
                "HLS: variant[{i}]: bw={} name={} uri={} codecs={}",
                bw, name, v.uri, codecs
            );
        }
        let chosen_index = initial_variant_index
            .filter(|&i| i < master.variants.len())
            .unwrap_or_else(|| select_variant_index(master.variants.as_slice()));
        let chosen = &master.variants[chosen_index];
        info!(
            "HLS: selecting variant index {} of {} (initial={:?}) -> uri={} bw={:?} name={:?} codecs={:?}",
            chosen_index,
            master.variants.len(),
            initial_variant_index,
            chosen.uri,
            chosen.bandwidth,
            chosen.name,
            chosen.codec.as_ref().and_then(|c| c.codecs.clone()),
        );

        // Build ABR controller and initialize (this will select the initial variant internally).
        let init_bw = master.variants[chosen_index].bandwidth.unwrap_or(0) as f64;
        let mut controller = AbrController::new(manager, abr_config.clone(), initial_mode, chosen_index, init_bw);
        if let Err(e) = controller.init().await {
            error!("HLS: controller init failed: {e:?}");
            return;
        }



        // Send init segment for the initially selected variant (if any).
        match controller.inner_stream_mut().download_init_segment().await {
            Ok(Some(bytes)) => {
                trace!("HLS: init segment fetched: bytes={}", bytes.len());
                if data_tx.send(bytes).is_err() {
                    trace!("HLS: consumer dropped while sending init segment; stopping producer");
                    return;
                }
            }
            Ok(None) => {
                debug!("HLS: no init segment present");
            }
            Err(e) => {
                error!("HLS: failed to download init segment: {e:?}");
                // Non-fatal.
            }
        }

        // Track current variant; on change, send a fresh init segment before media bytes.
        let mut last_variant_id: Option<VariantId> = controller.current_variant_id();

        // Stream media segments until EOF or stop signal.
        loop {
            if cancel_child.is_cancelled() {
                trace!("HLS: cancel requested, stopping producer");
                break;
            }

            match controller.next_segment().await {
                Ok(Some(seg)) => {
                    // If variant switched, send its init segment before first media packet.
                    if Some(seg.variant_id) != last_variant_id {
                        trace!(
                            "HLS: variant switch detected: {:?} -> {:?}",
                            last_variant_id,
                            seg.variant_id
                        );
                        match controller.inner_stream_mut().download_init_segment().await {
                            Ok(Some(bytes)) => {
                                if data_tx.send(bytes).is_err() {
                                    trace!("HLS: consumer dropped while sending switched init; stopping producer");
                                    break;
                                }
                            }
                            Ok(None) => {
                                trace!("HLS: switched variant has no init segment");
                            }
                            Err(e) => {
                                error!("HLS: failed to download switched init segment: {e:?}");
                            }
                        }
                        last_variant_id = Some(seg.variant_id);
                    }

                    // Convert Bytes to Vec<u8> and forward.
                    let chunk_len = seg.data.len();
                    let variant = seg.variant_id.0;
                    let dur_ms = seg.duration.as_millis();
                    let codec = seg.codec_info.as_ref().and_then(|c| c.codecs.clone());
                    trace!(
                        "HLS: sending segment seq={} bytes={} variant={} dur_ms={} codec={:?}",
                        seg.sequence, chunk_len, variant, dur_ms, codec
                    );
                    if data_tx.send(seg.data.to_vec()).is_err() {
                        trace!("HLS: consumer dropped; stopping producer");
                        break;
                    }
                }
                Ok(None) => {
                    // End of VOD playlist.
                    trace!("HLS: end of stream reached (playlist ENDLIST or no more segments)");
                    break;
                }
                Err(e) => {
                    trace!("HLS: next_segment error: {e:?}");
                    // Decide whether to continue or break; for now, break on errors.
                    break;
                }
            }
        }
        });
    });

    trace!("HLS: worker spawn complete for URL: {}", url);

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
            cancel,
            join: Mutex::new(Some(join)),
            seek_flag,
        },
    ))
}

/// Variant selection heuristic:
/// - Prefer variants that declare audio codecs (mp4a, aac, ac-3, ec-3, opus, vorbis, flac, mp3),
///   picking the one with the lowest advertised bandwidth among them.
/// - If none declare audio explicitly, fall back to the absolute lowest-bandwidth variant.
/// - If bandwidth is missing across the board, fall back to index 0 (or first audio-declared).
fn select_variant_index(variants: &[VariantStream]) -> usize {
    if variants.is_empty() {
        return 0;
    }

    // First pass: collect variants that explicitly declare audio codecs.
    let mut audio_idxs: Vec<usize> = Vec::new();
    for (i, v) in variants.iter().enumerate() {
        let mut has_audio = false;
        if let Some(codec) = &v.codec {
            // Prefer explicit audio codec field when present.
            if let Some(ac) = &codec.audio_codec {
                if !ac.is_empty() {
                    has_audio = true;
                }
            }
            // Also inspect the raw CODECS string for common audio identifiers.
            if let Some(codecs_str) = &codec.codecs {
                let s = codecs_str.to_ascii_lowercase();
                const TOKENS: [&str; 8] = [
                    "mp4a", "aac", "ac-3", "ec-3", "opus", "vorbis", "flac", "mp3",
                ];
                if TOKENS.iter().any(|t| s.contains(t)) {
                    has_audio = true;
                }
            }
        }
        if has_audio {
            audio_idxs.push(i);
        }
    }

    // If we found audio-declared variants, choose the one with the lowest bandwidth.
    if !audio_idxs.is_empty() {
        let mut best_i = audio_idxs[0];
        let mut best_bw = variants[best_i].bandwidth.unwrap_or(u64::MAX);
        for &idx in &audio_idxs[1..] {
            let bw = variants[idx].bandwidth.unwrap_or(u64::MAX);
            if bw < best_bw {
                best_bw = bw;
                best_i = idx;
            }
        }
        return best_i;
    }

    // Fallback: choose the variant with the lowest bandwidth overall.
    let mut best_idx = 0usize;
    let mut best_bw = variants[0].bandwidth.unwrap_or(u64::MAX);
    for (i, v) in variants.iter().enumerate().skip(1) {
        let bw = v.bandwidth.unwrap_or(u64::MAX);
        if bw < best_bw {
            best_bw = bw;
            best_idx = i;
        }
    }
    best_idx
}
