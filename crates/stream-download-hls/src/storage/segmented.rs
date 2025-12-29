#![allow(clippy::type_complexity)]
//! Segmented storage provider for HLS.
//!
//! Splits an ordered `StreamMsg` stream into per-chunk files using `StreamControl` boundaries and
//! exposes a stitched `Read + Seek` view over the logical stream.
//!
//! Protocol constraint: bytes must only be written after `StreamControl::ChunkStart`.
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use super::cache_layer::HlsCacheLayer;
use super::hls_factory::HlsFileTreeSegmentFactory;
use super::tree_handle::TreeStorageResourceReader;
use super::{cache_layer, hls_factory};
use stream_download::source::{ChunkKind, ResourceKey, StreamControl};
use stream_download::storage::ProvidesStorageHandle;
use stream_download::storage::StorageHandle;
use stream_download::storage::StorageResourceReader;
use stream_download::storage::{
    ContentLength, DynamicLength, SegmentedLength, StorageProvider, StorageWriter,
};
use tracing::{info, trace};

/// Factory for creating one [`StorageProvider`] per chunk.
pub trait SegmentStorageFactory: Clone + Send + Sync + 'static {
    type Provider: StorageProvider + Send + 'static;

    /// Creates a storage provider for a specific chunk within `stream_key`.
    fn provider_for_segment(
        &self,
        stream_key: &ResourceKey,
        chunk_id: u64,
        kind: ChunkKind,
        filename_hint: Option<&str>,
    ) -> io::Result<Self::Provider>;
}

/// Segmented storage provider driven by `StreamControl` boundaries.
#[derive(Clone)]
pub struct SegmentedStorageProvider<F>
where
    F: SegmentStorageFactory,
{
    factory: F,
    default_stream_key: ResourceKey,
    /// Root directory used to persist small resources written via `StoreResource` and read via
    /// `StorageHandle` (tree-layout: `storage_root/<key-as-path>`).
    storage_root: PathBuf,
}

impl<F> SegmentedStorageProvider<F>
where
    F: SegmentStorageFactory,
{
    /// Creates a segmented storage provider that uses `factory` to create one inner provider per chunk.
    ///
    /// `storage_root` is used for `StoreResource` blobs and the tree-layout `StorageHandle`.
    pub fn new(factory: F, storage_root: impl Into<PathBuf>) -> Self {
        Self {
            factory,
            default_stream_key: ResourceKey("playback".into()),
            storage_root: storage_root.into(),
        }
    }

    /// Convenience constructor for persistent HLS caching under a single `storage_root`.
    pub fn new_hls_file_tree(
        storage_root: impl Into<PathBuf>,
        prefetch_bytes: NonZeroUsize,
        max_cached_streams: Option<NonZeroUsize>,
    ) -> SegmentedStorageProvider<HlsCacheLayer<HlsFileTreeSegmentFactory>> {
        // Best-effort ensure the root exists up front.
        let storage_root: PathBuf = storage_root.into();
        let _ = fs::create_dir_all(&storage_root);

        let file_tree = HlsFileTreeSegmentFactory::new(storage_root.clone(), prefetch_bytes);
        let mut factory = HlsCacheLayer::new(file_tree, storage_root.clone());
        if let Some(max) = max_cached_streams {
            factory = factory.with_max_cached_streams(max);
        }

        SegmentedStorageProvider::new(factory, storage_root)
    }

    // /// Override the default logical stream key used by the reader.
    // pub fn with_default_stream_key(mut self, key: ResourceKey) -> Self {
    //     self.default_stream_key = key;
    //     self
    // }
}

impl<F> StorageProvider for SegmentedStorageProvider<F>
where
    F: SegmentStorageFactory,
{
    type Reader = SegmentedReader<F::Provider>;
    type Writer = SegmentedWriter<F>;

    fn into_reader_writer(
        self,
        _content_length: ContentLength,
    ) -> io::Result<(Self::Reader, Self::Writer)> {
        let state = Arc::new(RwLock::new(SharedState::<F::Provider> {
            default_stream_key: self.default_stream_key.clone(),
            resources: HashMap::new(),
            streams: HashMap::new(),
            storage_root: self.storage_root.clone(),
        }));

        let reader = SegmentedReader::<F::Provider> {
            state: state.clone(),
            stream_key: self.default_stream_key.clone(),
            pos: 0,
        };

        let writer = SegmentedWriter::<F> {
            state,
            factory: self.factory,
            current_stream_key: None,
            current_seg_index: None,
            // current_kind: None,
        };

        Ok((reader, writer))
    }

    fn max_capacity(&self) -> Option<usize> {
        // Segmented provider capacity is dictated by the inner provider per segment.
        // Returning None lets upstream logic decide.
        None
    }
}

impl<F> ProvidesStorageHandle for SegmentedStorageProvider<F>
where
    F: SegmentStorageFactory,
{
    fn storage_handle(&self) -> Option<StorageHandle> {
        Some(TreeStorageResourceReader::new(self.storage_root.clone()).into_handle())
    }
}

/// Shared orchestration state for segmented storage.
struct SharedState<P>
where
    P: StorageProvider + Send + 'static,
{
    default_stream_key: ResourceKey,
    resources: HashMap<ResourceKey, Bytes>,
    streams: HashMap<ResourceKey, StreamState<P>>,
    storage_root: PathBuf,
}

/// Per-stream segmented state (sequence of segments, in order).
struct StreamState<P>
where
    P: StorageProvider + Send + 'static,
{
    segments: Vec<Arc<Segment<P>>>,
}

impl<P> Default for StreamState<P>
where
    P: StorageProvider + Send + 'static,
{
    fn default() -> Self {
        Self {
            segments: Vec::new(),
        }
    }
}

// (duplicate `Default` impl removed)

/// A single segment, backed by an inner provider's `(Reader, Writer)`.
struct Segment<P>
where
    P: StorageProvider + Send + 'static,
{
    /// Reader for this segment. Protected by a mutex to allow safe seek/read.
    reader: Mutex<P::Reader>,
    /// Writer for this segment, present until the segment is finalized.
    writer: Mutex<Option<P::Writer>>,
    /// Reported length (best-known before processing). When `None`, we fall back to gathered.
    reported: Option<u64>,
    /// Bytes written (actual gathered length so far).
    gathered_len: Mutex<u64>,
    /// Segment finalization flag.
    finalized: Mutex<bool>,
    /// Logical start offset within the underlying segment resource.
    ///
    /// When non-zero, the stitched reader will expose only the suffix of the segment starting at
    /// `start_offset` (i.e. it will not return bytes in `[0..start_offset)` to the consumer).
    ///
    /// This is used to support seek/skip into the middle of a cached segment without re-downloading.
    start_offset: u64,
}

impl<P> Segment<P>
where
    P: StorageProvider + Send + 'static,
{
    fn new(reader: P::Reader, writer: P::Writer, reported: Option<u64>, start_offset: u64) -> Self {
        Self {
            reader: Mutex::new(reader),
            writer: Mutex::new(Some(writer)),
            gathered_len: Mutex::new(0),
            finalized: Mutex::new(false),
            reported,
            start_offset,
        }
    }

    fn available_len(&self) -> u64 {
        // Expose only the readable suffix. If the segment is shorter than the start offset,
        // the visible length is 0.
        let gathered = *self.gathered_len.lock();
        gathered.saturating_sub(self.start_offset)
    }

    fn is_finalized(&self) -> bool {
        *self.finalized.lock()
    }
}

/// Reader that stitches per-segment readers of the current stream into a single logical stream.
pub struct SegmentedReader<P>
where
    P: StorageProvider + Send + 'static,
{
    state: Arc<RwLock<SharedState<P>>>,
    /// Last observed default stream key. The reader follows `SharedState::default_stream_key`,
    /// and resets `pos` when it changes.
    stream_key: ResourceKey,
    pos: u64,
}

impl<P> SegmentedReader<P>
where
    P: StorageProvider + Send + 'static,
{
    /// Ensure the reader follows the current shared default stream key.
    ///
    /// If the default key changes (via `StreamControl::SetDefaultStreamKey`), we reset `pos` to 0.
    #[inline]
    fn refresh_stream_key_from_state(&mut self) {
        let new_key = { self.state.read().default_stream_key.clone() };
        if self.stream_key != new_key {
            self.stream_key = new_key;
            self.pos = 0;
        }
    }

    /// Current logical stream key (last observed).
    pub fn stream_key(&self) -> &ResourceKey {
        &self.stream_key
    }
}

impl<P> Read for SegmentedReader<P>
where
    P: StorageProvider + Send + 'static,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.refresh_stream_key_from_state();

        if buf.is_empty() {
            return Ok(0);
        }

        // Storage readers in `stream_download` are expected to behave like normal blocking `Read`
        // handles: returning `Ok(0)` should mean true EOF. For segmented storage, the logical stream
        // grows over time; therefore, if we're at the current end but the last segment is not yet
        // finalized, we must NOT return `Ok(0)` (that would look like EOF to consumers).
        //
        // We don't have a condvar here (blocking is handled at a higher level via `SourceHandle`),
        // so the best we can do is surface an "try again later" signal.
        //
        // NOTE: If you observe busy-loops at call sites, wire this to the core wait/notify path.
        let (segments, cumulative_starts, total_len, last_finalized) = {
            let guard = self.state.read();
            let Some(stream) = guard.streams.get(&self.stream_key) else {
                // Stream not yet created; treat as "not ready" rather than EOF.
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "segmented stream not yet available",
                ));
            };

            let mut starts = Vec::with_capacity(stream.segments.len());
            let mut segs = Vec::with_capacity(stream.segments.len());
            let mut offset = 0u64;
            for seg in &stream.segments {
                starts.push(offset);
                let avail = seg.available_len();
                offset = offset.saturating_add(avail);
                segs.push(seg.clone());
            }

            let last_finalized = stream
                .segments
                .last()
                .map(|s| s.is_finalized())
                .unwrap_or(false);

            (segs, starts, offset, last_finalized)
        };

        if segments.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "segmented stream has no segments yet",
            ));
        }

        if self.pos >= total_len && !last_finalized {
            // We must only return `Ok(0)` (true EOF) when the whole logical stream is finished.
            // For segmented storage, "last segment finalized" is not sufficient to prove that
            // no more segments will appear later (e.g. more chunks may be created after the
            // currently-last one).
            //
            // So at end-of-available-bytes we always report "not ready" and let the higher level
            // (`stream_download`'s SourceHandle) block until more bytes become available or the
            // download completes.
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "segmented stream reached current end; awaiting more data or stream completion",
            ));
        }

        // Locate segment for current position.
        let mut seg_index = match cumulative_starts
            .iter()
            .enumerate()
            .take(segments.len())
            .rfind(|(_, start)| **start <= self.pos)
        {
            Some((idx, _)) => idx,
            None => 0,
        };

        let mut read_total = 0usize;
        let mut local_pos = self.pos;

        while read_total < buf.len() && seg_index < segments.len() {
            let seg = &segments[seg_index];
            let seg_start = cumulative_starts[seg_index];
            let seg_len = seg.available_len();
            if local_pos >= seg_start.saturating_add(seg_len) {
                seg_index += 1;
                continue;
            }

            let offset_in_seg = local_pos.saturating_sub(seg_start);
            let remaining_in_seg = seg_len.saturating_sub(offset_in_seg) as usize;
            let remaining_in_buf = buf.len() - read_total;
            let to_read = remaining_in_seg.min(remaining_in_buf);
            if to_read == 0 {
                break;
            }

            // Perform IO on the inner segment reader.
            {
                let mut inner = seg.reader.lock();
                let physical_offset = seg.start_offset.saturating_add(offset_in_seg);
                inner.seek(SeekFrom::Start(physical_offset))?;
                let n = inner.read(&mut buf[read_total..read_total + to_read])?;

                if n == 0 {
                    // If we expected bytes but got none, treat as "not ready" unless this is
                    // the final segment and it is finalized (true EOF is handled above).
                    //
                    // This prevents spurious `Ok(0)` returns that look like EOF to consumers.
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "segment reader returned 0 before stream EOF; bytes not yet readable",
                    ));
                }

                read_total += n;
                local_pos = local_pos.saturating_add(n as u64);
                trace!(
                    "read {} bytes from segment {} at position {}, total read {}",
                    n, seg_index, local_pos, read_total
                );
            }
        }

        self.pos = self.pos.saturating_add(read_total as u64);
        Ok(read_total)
    }
}

impl<P> Seek for SegmentedReader<P>
where
    P: StorageProvider + Send + 'static,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Start(p) => {
                self.pos = p;
                Ok(self.pos)
            }
            SeekFrom::Current(delta) => {
                if delta < 0 {
                    self.pos = self.pos.saturating_sub(delta.unsigned_abs());
                } else {
                    self.pos = self.pos.saturating_add(delta as u64);
                }
                Ok(self.pos)
            }
            SeekFrom::End(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "seek from end not supported in segmented reader",
            )),
        }
    }
}

/// Writer that orchestrates per-chunk writers via ordered `StreamControl` messages.
/// Implements `StorageWriter::control` to receive chunk boundaries and resources.
pub struct SegmentedWriter<F>
where
    F: SegmentStorageFactory,
{
    state: Arc<RwLock<SharedState<F::Provider>>>,
    factory: F,

    current_stream_key: Option<ResourceKey>,
    current_seg_index: Option<usize>,
    // current_kind: Option<ChunkKind>,
}

impl<F> SegmentedWriter<F>
where
    F: SegmentStorageFactory,
{
    fn ensure_stream(&self, key: &ResourceKey) {
        let mut guard = self.state.write();
        guard
            .streams
            .entry(key.clone())
            .or_insert_with(StreamState::<F::Provider>::default);
    }

    fn open_new_segment(
        &mut self,
        stream_key: ResourceKey,
        reported: Option<u64>,
        kind: ChunkKind,
        filename_hint: Option<Arc<str>>,
        start_offset: u64,
    ) -> io::Result<()> {
        self.ensure_stream(&stream_key);

        let content_length = reported
            .map(|r| {
                ContentLength::Dynamic(DynamicLength {
                    reported: r,
                    gathered: None,
                })
            })
            .unwrap_or(ContentLength::Unknown);

        // Determine chunk index within the stream_key.
        let chunk_id: u64 = {
            let guard = self.state.read();
            guard
                .streams
                .get(&stream_key)
                .map(|s| s.segments.len() as u64)
                .unwrap_or(0)
        };

        trace!(
            "storage: ChunkStart stream_key='{}' chunk_id={} kind={:?} reported_len={:?} filename_hint={:?}",
            stream_key.0,
            chunk_id,
            kind,
            reported,
            filename_hint.as_deref()
        );

        let provider = self.factory.provider_for_segment(
            &stream_key,
            chunk_id,
            kind,
            filename_hint.as_deref(),
        )?;

        trace!(
            "storage: segment provider created stream_key='{}' chunk_id={} kind={:?}",
            stream_key.0, chunk_id, kind
        );

        let (reader, writer) = provider.into_reader_writer(content_length)?;
        let segment = Arc::new(Segment::<F::Provider>::new(
            reader,
            writer,
            reported,
            start_offset,
        ));

        let mut guard = self.state.write();
        let stream = guard
            .streams
            .get_mut(&stream_key)
            .expect("stream must exist after ensure_stream");
        stream.segments.push(segment);
        self.current_stream_key = Some(stream_key);
        self.current_seg_index = Some(stream.segments.len() - 1);
        // self.current_kind = Some(kind);
        Ok(())
    }

    fn active_stream_key(&self) -> ResourceKey {
        let guard = self.state.read();
        self.current_stream_key
            .clone()
            .unwrap_or_else(|| guard.default_stream_key.clone())
    }

    fn current_segment(&self) -> io::Result<Arc<Segment<F::Provider>>> {
        let guard = self.state.read();
        let stream_key = self.active_stream_key();
        let stream = guard.streams.get(&stream_key).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "stream not found while resolving current segment",
            )
        })?;

        let Some(seg_index) = self.current_seg_index else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "no active segment; expected ChunkStart before data",
            ));
        };

        if seg_index >= stream.segments.len() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "active segment index out of bounds",
            ));
        }

        Ok(stream.segments[seg_index].clone())
    }

    fn finalize_current_segment(&mut self, gathered_len: u64) -> io::Result<()> {
        let seg = self.current_segment()?;

        // Use the readable length from the segment reader as the authoritative value.
        //
        // We have seen cases where the worker/writer reports one more byte than is actually
        // readable via the stitched `Read + Seek` view, which causes stalls / EOF-ish behavior
        // and makes StreamControl accounting exceed real readable bytes.
        //
        // Note: the stitched stream exposes only the suffix starting at `start_offset`, so
        // we must express the chosen gathered length in *underlying segment coordinates*
        // (reader length includes the full segment resource).
        let actual_readable = {
            let mut r = seg.reader.lock();
            match r.seek(SeekFrom::End(0)) {
                Ok(end) => end,
                Err(_) => gathered_len,
            }
        };

        let chosen_gathered_len = actual_readable.min(gathered_len);

        if chosen_gathered_len != gathered_len {
            tracing::warn!(
                reported_gathered_len = gathered_len,
                actual_readable_len = actual_readable,
                start_offset = seg.start_offset,
                "storage: ChunkEnd gathered_len exceeds readable bytes; clamping to readable length"
            );
        }

        {
            let mut g = seg.gathered_len.lock();
            *g = chosen_gathered_len;
        }

        trace!(
            "storage: ChunkEnd gathered_len={} (finalizing segment)",
            chosen_gathered_len
        );

        if let Some(mut inner) = seg.writer.lock().take() {
            // Best-effort flush; truncation is not part of core StorageWriter contract today.
            let _ = inner.flush();
        }
        *seg.finalized.lock() = true;

        // Keep current_seg_index as-is; the next ChunkStart will open the next one.
        Ok(())
    }

    fn store_resource(&self, key: ResourceKey, data: Bytes) -> io::Result<()> {
        trace!(
            "storage: StoreResource received key='{}' ({} bytes)",
            key.0,
            data.len()
        );

        // Keep an in-memory copy (useful for debugging / quick reads if we later add a handle).
        {
            let mut guard = self.state.write();
            guard.resources.insert(key.clone(), data.clone());
        }

        // Persist to disk using the same tree-layout rules as our StorageHandle:
        // `storage_root / <key-as-path>`
        //
        // Important:
        // - `ResourceKey` is treated as a relative path (slash-separated).
        // - Each component is sanitized to avoid traversal and weird filesystem edge cases.
        use std::fs;
        use std::fs::OpenOptions;
        use std::io::Write as _;

        let root = { self.state.read().storage_root.clone() };

        let rel = {
            let mut pb = PathBuf::new();
            for comp in key.0.split('/') {
                // Disallow traversal-ish components.
                if comp == "." || comp == ".." {
                    continue;
                }

                // Keep component stable and safe on disk.
                let clean: String = comp
                    .chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                            c
                        } else {
                            '_'
                        }
                    })
                    .collect();

                let clean = clean.trim_matches('_');
                if !clean.is_empty() {
                    pb.push(clean);
                }
            }

            if pb.as_os_str().is_empty() {
                pb.push("default");
            }
            pb
        };

        let path = root.join(rel);
        trace!("storage: StoreResource write path='{}'", path.display());

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        f.write_all(&data)?;
        f.flush()?;

        trace!(
            "storage: StoreResource persisted key='{}' ({} bytes) -> '{}'",
            key.0,
            data.len(),
            path.display()
        );

        Ok(())
    }
}

impl<F> Write for SegmentedWriter<F>
where
    F: SegmentStorageFactory,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.current_seg_index.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "received data without active chunk; expected StreamControl::ChunkStart before data",
            ));
        }

        let seg = self.current_segment()?;
        if seg.is_finalized() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "received data after chunk was finalized",
            ));
        }

        let mut guard = seg.writer.lock();
        let Some(inner) = guard.as_mut() else {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "chunk writer missing while chunk is not finalized",
            ));
        };

        let offset_in_seg = inner.seek(SeekFrom::Current(0))?;
        let written = inner.write(buf)?;
        inner.flush()?;

        // Update gathered length = max(existing, offset + written)
        {
            let mut g = seg.gathered_len.lock();
            let new_gathered = offset_in_seg.saturating_add(written as u64);
            if new_gathered > *g {
                *g = new_gathered;
            }
        }

        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<F> Seek for SegmentedWriter<F>
where
    F: SegmentStorageFactory,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        // IMPORTANT:
        // `stream_download` calls `writer.stream_position()` during prefetch.
        // `stream_position()` is implemented via `seek(SeekFrom::Current(0))`.
        //
        // For segmented writers it is valid to query the current logical position even after a
        // chunk has been finalized, so we must support `SeekFrom::Current(0)` in all states.
        if matches!(pos, SeekFrom::Current(0)) {
            let stream_key = self.active_stream_key();
            let seg_index = self.current_seg_index;

            let guard = self.state.read();
            let stream = guard.streams.get(&stream_key).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "stream not found while seeking writer",
                )
            })?;

            // Sum lengths of segments before the active one.
            let mut abs = 0u64;
            if let Some(i) = seg_index {
                for seg in stream.segments.iter().take(i) {
                    abs = abs.saturating_add(seg.available_len());
                }

                // Add position within current segment:
                // - if still writable: use inner writer position
                // - if finalized: snap to end of segment
                if let Some(seg) = stream.segments.get(i) {
                    if seg.writer.lock().is_some() {
                        // IMPORTANT:
                        // `stream_download` uses `writer.stream_position()` (implemented via
                        // `seek(SeekFrom::Current(0))`) to decide which byte ranges are readable
                        // and to wake blocked reads.
                        //
                        // For segmented storage the authoritative "readable so far" value is the
                        // gathered length (what has actually been written and is expected to be
                        // readable via the stitched reader), NOT the current cursor position of
                        // the underlying writer handle (which may advance via seeks or buffering).
                        //
                        // If we report cursor position here, core may mark bytes as downloaded and
                        // unblock readers too early, causing the stitched reader to return `Ok(0)`
                        // ("EOF-ish") before bytes are actually readable.
                        abs = abs.saturating_add(seg.available_len());
                    } else {
                        // Finalized segment: readable length is stable.
                        abs = abs.saturating_add(seg.available_len());
                    }
                }
            } else {
                // No active segment selected yet; treat as position 0.
            }

            return Ok(abs);
        }

        // For correctness, only allow seeking within the current active (non-finalized) segment.
        let seg = self.current_segment()?;
        if seg.is_finalized() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "writer seek not supported after chunk finalization",
            ));
        }

        let mut guard = seg.writer.lock();
        let Some(inner) = guard.as_mut() else {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "chunk writer missing",
            ));
        };

        let new_pos = inner.seek(pos)?;
        // Update gathered length pessimistically to avoid shrinking visible length.
        {
            let mut g = seg.gathered_len.lock();
            if new_pos > *g {
                *g = new_pos;
            }
        }
        Ok(new_pos)
    }
}

impl<F> StorageWriter for SegmentedWriter<F>
where
    F: SegmentStorageFactory,
{
    fn control(&mut self, msg: StreamControl) -> io::Result<()> {
        match msg {
            StreamControl::ChunkStart {
                stream_key,
                kind,
                reported_len,
                filename_hint,
                start_offset,
                ..
            } => self.open_new_segment(stream_key, reported_len, kind, filename_hint, start_offset),
            StreamControl::ChunkEnd { gathered_len, .. } => {
                self.finalize_current_segment(gathered_len)
            }
            StreamControl::StoreResource { key, data } => self.store_resource(key, data),
            StreamControl::SetDefaultStreamKey { stream_key } => {
                // Update the shared default stream key so stitched readers can follow it without
                // requiring a direct reader-side API call.
                //
                // Note: we intentionally do not mutate writer-side cursor here. The next
                // ChunkStart will select the stream explicitly.
                let mut guard = self.state.write();
                guard.default_stream_key = stream_key;
                Ok(())
            }
        }
    }

    fn get_available_ranges_for(
        &mut self,
        stream_key: &ResourceKey,
    ) -> io::Result<Option<(ContentLength, Vec<Range<u64>>)>> {
        let guard = self.state.read();
        let Some(stream) = guard.streams.get(stream_key) else {
            return Ok(None);
        };
        if stream.segments.is_empty() {
            return Ok(None);
        }

        let mut ranges = Vec::<Range<u64>>::with_capacity(stream.segments.len());
        let mut seg_lengths = Vec::<DynamicLength>::with_capacity(stream.segments.len());
        let mut offset = 0u64;

        for seg in &stream.segments {
            let gathered = seg.available_len();
            let reported = seg.reported.unwrap_or(gathered);

            seg_lengths.push(DynamicLength {
                reported,
                gathered: Some(gathered),
            });

            let start = offset;
            let end = start.saturating_add(gathered);
            ranges.push(start..end);
            offset = end;
        }

        Ok(Some((
            ContentLength::Segmented(SegmentedLength {
                segments: seg_lengths,
            }),
            ranges,
        )))
    }
}
