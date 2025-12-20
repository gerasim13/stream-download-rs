#![allow(clippy::type_complexity)]
//! Segmented storage provider for HLS.
//!
//! This module implements a `stream_download::storage::StorageProvider` wrapper that understands
//! ordered [`StreamControl`](stream_download::source::StreamControl) messages emitted by an HLS
//! [`SourceStream`](stream_download::source::SourceStream).
//!
//! The goal is to split the logical byte stream into **per-chunk** files (typically one init
//! segment + many media segments), while still exposing a single contiguous `Read + Seek` view to
//! decoders (e.g. Symphonia) via a virtual stitched reader.
//!
//! Design notes:
//! - All segmentation is driven by **ordered** `StreamMsg::Control(StreamControl::...)` messages.
//!   There is no separate channel for control vs data, so ordering is deterministic.
//! - Each chunk is backed by a per-segment `StorageProvider` created by a factory.
//! - Multiple logical streams are supported by `stream_key` (e.g. different variants/codecs).
//! - Auxiliary resources (playlists, keys, etc.) can be stored via `StoreResource` and retrieved
//!   via a tree-layout `StorageHandle` under `storage_root`.
//!
//! Important:
//! - This is intentionally scoped to HLS for now. If later you want a generic segmented stream
//!   facility, we can extract it into `stream-download`.
//! - The writer is intentionally strict: **bytes must only be written after `ChunkStart`**.
//!   If HLS emits data without an active chunk, we return an error to avoid corrupt outputs.

pub mod hls_factory;

pub mod cache_layer;

pub mod tree_handle;

use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use stream_download::source::{ChunkKind, ResourceKey, StreamControl};
use stream_download::storage::{ContentLength, DynamicLength, StorageProvider, StorageWriter};

/// Optional integration: if the core provides these types/traits (they existed in earlier WIP),
/// `SegmentedStorageProvider` can vend a handle to read stored resources.
///
/// If your current `stream-download` core does not have these, keep using the provider without
/// resource retrieval for now, or add the handle types later.
///
/// We keep this behind a feature-like cfg by using `cfg(any())` as a placeholder. Replace it with
/// a real feature flag if you want.
///
/// NOTE: This file is added "from scratch" for the current repo state; it intentionally avoids
/// depending on non-existent core APIs.
#[cfg(any())]
use stream_download::storage::{ProvidesStorageHandle, StorageHandle, StorageResourceReader};

/// Factory for creating a fresh per-segment [`StorageProvider`] instance.
///
/// This is required for persistent caching with deterministic file layouts:
/// a plain `P: StorageProvider` instance represents one backing resource, so we must be able to
/// create a *new* provider for each segment based on (`stream_key`, `chunk_id`, `kind`, `filename_hint`).
pub trait SegmentStorageFactory: Clone + Send + Sync + 'static {
    type Provider: StorageProvider + Send + 'static;

    /// Create a storage provider for a specific segment.
    ///
    /// `chunk_id` is a monotonically increasing index within a `stream_key`.
    /// `filename_hint` is an optional playlist-derived basename to use "as is".
    fn provider_for_segment(
        &self,
        stream_key: &ResourceKey,
        chunk_id: u64,
        kind: ChunkKind,
        filename_hint: Option<&str>,
    ) -> io::Result<Self::Provider>;
}

/// A segmented storage provider.
///
/// Wraps a [`SegmentStorageFactory`] and orchestrates per-chunk inner providers based on
/// `StreamControl`.
///
/// - A fresh `F::Provider` is created per chunk via the factory.
/// - Each chunk gets its own `(Reader, Writer)` pair.
/// - A `SegmentedReader` stitches chunks for a chosen `stream_key`.
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
    /// Create a new segmented storage provider that uses `factory` to create a fresh inner provider
    /// for each chunk.
    ///
    /// `storage_root` is mandatory to prevent silent cache root mismatches between:
    /// - segment files (file-tree layout),
    /// - eviction/lease state,
    /// - `StoreResource` persisted blobs and `StorageHandle` reads.
    pub fn new(factory: F, storage_root: impl Into<PathBuf>) -> Self {
        Self {
            factory,
            default_stream_key: ResourceKey("playback".into()),
            storage_root: storage_root.into(),
        }
    }

    /// One-shot constructor for persistent HLS caching that wires **one** `storage_root` across:
    /// - the file-tree segment layout factory,
    /// - the HLS cache policy layer (leases + eviction),
    /// - the segmented storage provider (resources + `StorageHandle`).
    ///
    /// This avoids accidental root mismatches between the three components.
    pub fn new_hls_file_tree(
        storage_root: impl Into<PathBuf>,
        prefetch_bytes: NonZeroUsize,
        max_cached_streams: Option<NonZeroUsize>,
    ) -> SegmentedStorageProvider<cache_layer::HlsCacheLayer<hls_factory::HlsFileTreeSegmentFactory>>
    {
        let storage_root: PathBuf = storage_root.into();

        let file_tree =
            hls_factory::HlsFileTreeSegmentFactory::new(storage_root.clone(), prefetch_bytes);

        let mut factory = cache_layer::HlsCacheLayer::new(file_tree, storage_root.clone());
        if let Some(max) = max_cached_streams {
            factory = factory.with_max_cached_streams(max);
        }

        SegmentedStorageProvider::new(factory, storage_root)
    }

    /// Override the default logical stream key used by the reader.
    pub fn with_default_stream_key(mut self, key: ResourceKey) -> Self {
        self.default_stream_key = key;
        self
    }
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

        // Ensure there is a default stream entry so the reader has a target.
        {
            let mut guard = state.write();
            guard
                .streams
                .entry(self.default_stream_key.clone())
                .or_insert_with(StreamState::<F::Provider>::default);
        }

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
            current_kind: None,
        };

        Ok((reader, writer))
    }

    fn max_capacity(&self) -> Option<usize> {
        // Segmented provider capacity is dictated by the inner provider per segment.
        // Returning None lets upstream logic decide.
        None
    }
}

impl<F> stream_download::storage::ProvidesStorageHandle for SegmentedStorageProvider<F>
where
    F: SegmentStorageFactory,
{
    fn storage_handle(&self) -> Option<stream_download::storage::StorageHandle> {
        Some(
            crate::storage::tree_handle::TreeStorageResourceReader::new(self.storage_root.clone())
                .into_handle(),
        )
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

#[cfg(any())]
struct ResourceFsReader {
    root: PathBuf,
}

#[cfg(any())]
impl StorageResourceReader for ResourceFsReader {
    fn read(&self, key: &ResourceKey) -> io::Result<Option<Bytes>> {
        use std::fs;

        let rel = encode_resource_key(key);
        let path = self.root.join(rel);
        if !path.exists() {
            return Ok(None);
        }
        let data = fs::read(path)?;
        Ok(Some(Bytes::from(data)))
    }
}

#[cfg(any())]
fn encode_resource_key(key: &ResourceKey) -> PathBuf {
    let mut pb = PathBuf::new();
    for comp in key.0.split('/') {
        let clean = sanitize_component(comp);
        if !clean.is_empty() {
            pb.push(clean);
        }
    }
    if pb.as_os_str().is_empty() {
        pb.push("default");
    }
    pb
}

#[cfg(any())]
fn sanitize_component(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
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
    /// Chunk kind (Init/Media) for debugging / informational use.
    kind: ChunkKind,
    /// Optional filename hint for deterministic per-segment naming in file-based inners.
    filename_hint: Option<Arc<str>>,
}

impl<P> Segment<P>
where
    P: StorageProvider + Send + 'static,
{
    fn new(
        reader: P::Reader,
        writer: P::Writer,
        reported: Option<u64>,
        kind: ChunkKind,
        filename_hint: Option<Arc<str>>,
    ) -> Self {
        Self {
            reader: Mutex::new(reader),
            writer: Mutex::new(Some(writer)),
            reported,
            gathered_len: Mutex::new(0),
            finalized: Mutex::new(false),
            kind,
            filename_hint,
        }
    }

    fn available_len(&self) -> u64 {
        *self.gathered_len.lock()
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

        // Snapshot segments and their cumulative offsets and available lengths.
        let (segments, cumulative_starts, total_len) = {
            let guard = self.state.read();
            let Some(stream) = guard.streams.get(&self.stream_key) else {
                return Ok(0);
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
            (segs, starts, offset)
        };

        if segments.is_empty() || self.pos >= total_len {
            return Ok(0);
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
                inner.seek(SeekFrom::Start(offset_in_seg))?;
                let n = inner.read(&mut buf[read_total..read_total + to_read])?;
                read_total += n;
                local_pos = local_pos.saturating_add(n as u64);
                if n == 0 {
                    // Defensive: if inner returned EOF unexpectedly, advance.
                    seg_index += 1;
                    continue;
                }
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
/// Actual bytes are written to the current chunk's inner writer.
///
/// This implements `StorageWriter::control` to receive chunk boundaries and resources.
pub struct SegmentedWriter<F>
where
    F: SegmentStorageFactory,
{
    state: Arc<RwLock<SharedState<F::Provider>>>,
    factory: F,

    current_stream_key: Option<ResourceKey>,
    current_seg_index: Option<usize>,
    current_kind: Option<ChunkKind>,
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

        let provider = self.factory.provider_for_segment(
            &stream_key,
            chunk_id,
            kind,
            filename_hint.as_deref(),
        )?;

        let (reader, writer) = provider.into_reader_writer(content_length)?;
        let segment = Arc::new(Segment::<F::Provider>::new(
            reader,
            writer,
            reported,
            kind,
            filename_hint,
        ));

        let mut guard = self.state.write();
        let stream = guard
            .streams
            .get_mut(&stream_key)
            .expect("stream must exist after ensure_stream");
        stream.segments.push(segment);
        self.current_stream_key = Some(stream_key);
        self.current_seg_index = Some(stream.segments.len() - 1);
        self.current_kind = Some(kind);
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
        {
            let mut g = seg.gathered_len.lock();
            *g = gathered_len;
        }

        if let Some(mut inner) = seg.writer.lock().take() {
            // Best-effort flush; truncation is not part of core StorageWriter contract today.
            let _ = inner.flush();
        }
        *seg.finalized.lock() = true;

        // Keep current_seg_index as-is; the next ChunkStart will open the next one.
        Ok(())
    }

    fn store_resource(&self, key: ResourceKey, data: Bytes) -> io::Result<()> {
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
                    if let Some(inner) = seg.writer.lock().as_mut() {
                        let inner_pos = inner.seek(SeekFrom::Current(0))?;
                        abs = abs.saturating_add(inner_pos);
                    } else {
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
            } => self.open_new_segment(stream_key, reported_len, kind, filename_hint),

            StreamControl::ChunkEnd {
                stream_key: _,
                kind: _,
                gathered_len,
            } => self.finalize_current_segment(gathered_len),

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
}

/// Optional helper method (not part of core traits) to report cached ranges for a stream.
///
/// This is useful for debugging and for later “smart seek” behaviors.
/// We keep it as an inherent method so we don't need to change core traits again.
impl<F> SegmentedWriter<F>
where
    F: SegmentStorageFactory,
{
    /// Returns a synthetic segmented `ContentLength` and a list of cached ranges for `stream_key`.
    pub fn get_cached_ranges_for(
        &self,
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
        let mut lengths = Vec::<DynamicLength>::with_capacity(stream.segments.len());
        let mut offset = 0u64;

        for seg in &stream.segments {
            let gathered = seg.available_len();
            let reported = seg.reported.unwrap_or(gathered);
            lengths.push(DynamicLength {
                reported,
                gathered: Some(gathered),
            });

            let start = offset;
            let end = start.saturating_add(gathered);
            ranges.push(start..end);
            offset = end;
        }

        // Core `ContentLength` currently has Static/Dynamic/Unknown in `5c6cadb`.
        // In `47e49d4` there was a `ContentLength::Segmented`. We cannot assume it exists now.
        //
        // To keep this file self-contained and compiling against current core, we return a Dynamic
        // length with reported = sum(reported) and gathered = sum(gathered). This is enough for
        // capacity planning and basic introspection.
        let sum_reported = lengths.iter().map(|d| d.reported).sum::<u64>();
        let sum_gathered = lengths
            .iter()
            .map(|d| d.gathered.unwrap_or(d.reported))
            .sum::<u64>();

        Ok(Some((
            ContentLength::Dynamic(DynamicLength {
                reported: sum_reported,
                gathered: Some(sum_gathered),
            }),
            ranges,
        )))
    }
}
