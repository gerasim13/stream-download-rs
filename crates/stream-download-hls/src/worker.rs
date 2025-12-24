use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use url::Url;

use crate::{
    AbrConfig, AbrController, HlsErrorKind, HlsManager, HlsStreamError, ResourceDownloader,
    StreamEvent, StreamMiddleware, apply_middlewares,
};

#[cfg(feature = "aes-decrypt")]
use crate::Aes128CbcMiddleware;
use stream_download::source::{ChunkKind, ResourceKey, StreamControl, StreamMsg};
use stream_download::storage::{DynamicLength, SegmentedLength, StorageHandle};

enum RaceOutcome<T> {
    Completed(T),
    Seek(u64),
    ChannelClosed,
}

pub struct HlsStreamWorker {
    data_sender: mpsc::Sender<StreamMsg>,
    seek_receiver: mpsc::Receiver<u64>,
    cancel_token: CancellationToken,
    event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
    controller: AbrController<HlsManager>,
    current_position: u64,
    bytes_to_skip: u64,
    retry_delay: std::time::Duration,

    // Storage handle for read-before-fetch caching probes (segments, playlists/keys in manager).
    storage_handle: StorageHandle,

    /// Best-effort segmented length snapshot shared with `HlsStream::content_length()`.
    segmented_length: Arc<std::sync::RwLock<SegmentedLength>>,

    // Stable identifier for persistent cache layout:
    // `<cache_root>/<master_hash>/<variant_id>/<segment_basename>`
    master_hash: Arc<str>,
}

impl HlsStreamWorker {
    #[inline]
    fn update_last_segment_length(&self, gathered_len: u64) {
        if let Ok(mut guard) = self.segmented_length.write() {
            if let Some(last) = guard.segments.last_mut() {
                last.gathered = Some(gathered_len);
                if last.reported == 0 {
                    last.reported = gathered_len;
                }
            }
        }
    }

    /// Build per-segment middlewares (no self borrow to avoid conflicts).
    #[inline]
    fn build_middlewares(
        drm_params_opt: Option<([u8; 16], [u8; 16])>,
    ) -> Vec<Arc<dyn StreamMiddleware>> {
        let mut v: Vec<Arc<dyn StreamMiddleware>> = Vec::new();

        #[cfg(feature = "aes-decrypt")]
        if let Some((key, iv)) = drm_params_opt {
            v.push(Arc::new(Aes128CbcMiddleware::new(key, iv)));
        }

        v
    }
    const INITIAL_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
    const MAX_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

    /// Emit events for variant changes and segment boundaries before streaming bytes.
    /// Keeps `run` method concise and standardizes event ordering.
    #[inline]
    fn emit_pre_segment_events(
        &self,
        last_variant_id: &mut Option<crate::model::VariantId>,
        desc: &crate::manager::SegmentDescriptor,
        seg_size: Option<u64>,
        init_len_opt: Option<u64>,
    ) {
        if *last_variant_id != Some(desc.variant_id) {
            let _ = self.event_sender.send(StreamEvent::VariantChanged {
                variant_id: desc.variant_id,
                codec_info: desc.codec_info.clone(),
            });
            *last_variant_id = Some(desc.variant_id);
        }
        if desc.is_init {
            let _ = self.event_sender.send(StreamEvent::InitStart {
                variant_id: desc.variant_id,
                codec_info: desc.codec_info.clone(),
                byte_len: init_len_opt,
            });
        } else {
            let _ = self.event_sender.send(StreamEvent::SegmentStart {
                sequence: desc.sequence,
                variant_id: desc.variant_id,
                byte_len: seg_size,
                duration: desc.duration,
            });
        }
    }

    /// Emit post-segment boundary events after streaming completes.
    #[inline]
    fn emit_post_segment_events(&self, desc: &crate::manager::SegmentDescriptor) {
        if desc.is_init {
            let _ = self.event_sender.send(StreamEvent::InitEnd {
                variant_id: desc.variant_id,
            });
        } else {
            let _ = self.event_sender.send(StreamEvent::SegmentEnd {
                sequence: desc.sequence,
                variant_id: desc.variant_id,
            });
        }
    }

    /// Apply a seek position updating internal byte counters and manager state.
    #[instrument(skip(self), fields(position))]
    async fn apply_seek_position(&mut self, position: u64) -> Result<(), HlsStreamError> {
        if position >= self.current_position {
            self.bytes_to_skip = position - self.current_position;
        } else {
            let (desc, intra) = self
                .controller
                .inner_stream_mut()
                .resolve_position(position)
                .await
                .map_err(|_| HlsStreamError::Hls(HlsErrorKind::SeekFailed))?;
            self.controller
                .inner_stream_mut()
                .seek_to_descriptor(&desc)
                .map_err(|_| HlsStreamError::Hls(HlsErrorKind::SeekFailed))?;
            self.bytes_to_skip = intra;
            self.current_position = position.saturating_sub(intra);
        }
        Ok(())
    }

    /// Compute segment size for skip math, probing when unknown.
    #[instrument(skip(self, desc))]
    async fn compute_segment_size(
        &mut self,
        desc: &crate::manager::SegmentDescriptor,
    ) -> Option<u64> {
        // Best-effort: if the segment already exists in persistent storage, reuse its length to
        // avoid a network HEAD probe.
        let storage_len = {
            // Build the same key used by the cache probe path below.
            let filename_hint = {
                let uri = desc.uri.as_str();
                let no_query = uri.split('?').next().unwrap_or(uri);
                no_query.rsplit('/').next().map(|s| Arc::<str>::from(s))
            };
            filename_hint.as_deref().and_then(|base| {
                let seg_key: ResourceKey =
                    format!("{}/{}", self.master_hash, desc.variant_id.0).into();
                let full_key: ResourceKey = format!("{}/{}", seg_key.0, base).into();
                self.storage_handle.len(&full_key).ok().flatten()
            })
        };

        if let Some(len) = storage_len {
            // Record the observed length to avoid future probes.
            if desc.is_init {
                self.controller.inner_stream_mut().set_segment_size(0, len);
            } else {
                self.controller
                    .inner_stream_mut()
                    .set_segment_size(desc.sequence, len);
            }
            return Some(len);
        }

        let seg_size_opt = if desc.is_init {
            self.controller.inner_stream().segment_size(0)
        } else {
            self.controller.inner_stream().segment_size(desc.sequence)
        };
        if let Some(sz) = seg_size_opt {
            Some(sz)
        } else {
            match self
                .controller
                .inner_stream_mut()
                .probe_and_record_segment_size(
                    if desc.is_init { 0 } else { desc.sequence },
                    &desc.uri,
                )
                .await
            {
                Ok(s) => s,
                Err(_) => None,
            }
        }
    }

    /// Resolve DRM (AES-128-CBC) params for a descriptor if applicable.
    #[instrument(skip(self, desc))]
    async fn resolve_drm_params_for_desc(
        &mut self,
        desc: &crate::manager::SegmentDescriptor,
    ) -> Option<([u8; 16], [u8; 16])> {
        #[cfg(not(feature = "aes-decrypt"))]
        return None;
        #[cfg(feature = "aes-decrypt")]
        return self
            .controller
            .inner_stream_mut()
            .resolve_aes128_cbc_params(
                desc.key.as_ref(),
                if desc.is_init {
                    None
                } else {
                    Some(desc.sequence)
                },
            )
            .await
            .ok()
            .flatten();
    }

    /// Handle live refresh wait with cancellation and seek support.
    #[instrument(skip(self), fields(wait = ?wait))]
    async fn handle_needs_refresh(
        &mut self,
        wait: std::time::Duration,
    ) -> Result<(), HlsStreamError> {
        match Self::race_with_seek(
            &self.cancel_token,
            &mut self.seek_receiver,
            tokio::time::sleep(wait),
        )
        .await?
        {
            RaceOutcome::Completed(_) => {
                tracing::trace!("HLS stream: live refresh wait completed");
            }
            RaceOutcome::Seek(position) => {
                tracing::trace!("HLS streaming loop: received seek during wait");
                self.apply_seek_position(position).await?;
            }
            RaceOutcome::ChannelClosed => {
                // Seek channel closed; continue loop
            }
        }
        Ok(())
    }

    /// Sleep with backoff and cancellation; update retry delay.
    #[instrument(skip(self))]
    async fn backoff_sleep(&mut self) -> Result<(), HlsStreamError> {
        tokio::select! {
            biased;
            _ = self.cancel_token.cancelled() => {
                return Err(HlsStreamError::Cancelled);
            }
            _ = tokio::time::sleep(self.retry_delay) => {}
        }
        self.retry_delay = (self.retry_delay * 2).min(Self::MAX_RETRY_DELAY);
        Ok(())
    }

    #[instrument(skip(cancel, seeks, fut))]
    async fn race_with_seek<F, T>(
        cancel: &CancellationToken,
        seeks: &mut mpsc::Receiver<u64>,
        fut: F,
    ) -> Result<RaceOutcome<T>, HlsStreamError>
    where
        F: std::future::Future<Output = T>,
    {
        tokio::pin!(fut);
        let res = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return Err(HlsStreamError::Cancelled);
            }
            seek = seeks.recv() => {
                if let Some(mut position) = seek {
                    // Coalesce burst seeks: drain pending messages to keep only the latest position
                    while let Ok(next) = seeks.try_recv() {
                        position = next;
                    }
                    Ok(RaceOutcome::Seek(position))
                } else {
                    Ok(RaceOutcome::ChannelClosed)
                }
            }
            out = &mut fut => {
                Ok(RaceOutcome::Completed(out))
            }
        };
        res
    }

    /// Pump bytes from a segment stream to downstream with backpressure, cancellation and seek handling.
    #[instrument(skip(self, stream))]
    async fn pump_stream_chunks(
        &mut self,
        mut stream: crate::HlsByteStream,
    ) -> Result<u64, HlsStreamError> {
        let mut gathered_len: u64 = 0;

        loop {
            let next_fut = futures_util::StreamExt::next(&mut stream);
            match Self::race_with_seek(&self.cancel_token, &mut self.seek_receiver, next_fut)
                .await?
            {
                RaceOutcome::Seek(position) => {
                    tracing::trace!("HLS streaming loop: received seek during streaming");
                    self.apply_seek_position(position).await?;
                    // break to apply new position on next iteration
                    break;
                }
                RaceOutcome::ChannelClosed => {
                    tracing::trace!("HLS stream: seek channel closed");
                    break;
                }
                RaceOutcome::Completed(item) => {
                    match item {
                        Some(Ok(chunk)) => {
                            let len = chunk.len() as u64;
                            gathered_len = gathered_len.saturating_add(len);
                            // Best-effort keep segmented length snapshot in sync even mid-segment
                            // so callers can observe a non-zero content length during prefetch.
                            self.update_last_segment_length(gathered_len);

                            // Send chunk as ordered stream message.
                            if self.data_sender.send(StreamMsg::Data(chunk)).await.is_err() {
                                tracing::trace!("HLS stream: receiver dropped, stopping");
                                break;
                            }

                            self.current_position = self.current_position.saturating_add(len);
                        }
                        Some(Err(e)) => {
                            tracing::error!(
                                "Error reading segment chunk: {}, retrying in {:?}",
                                e,
                                self.retry_delay
                            );
                            self.backoff_sleep().await?;
                            break;
                        }
                        None => {
                            // Segment finished
                            break;
                        }
                    }
                }
            }
        }

        Ok(gathered_len)
    }

    /// Try to skip an entire segment if we know its size (best-effort).
    /// Returns `true` if the segment is fully skipped and the caller should stop processing it.
    fn try_skip_full_segment_by_size(&mut self, seg_size: Option<u64>) -> bool {
        if let (Some(size), true) = (seg_size, self.bytes_to_skip > 0) {
            if self.bytes_to_skip >= size {
                self.bytes_to_skip -= size;
                self.current_position += size;
                return true;
            }
        }
        false
    }

    /// If the variant changed, notify the storage layer so the stitched reader follows the
    /// new logical stream key.
    fn maybe_update_default_stream_key(
        &mut self,
        desc: &crate::manager::SegmentDescriptor,
        last_variant_id: &mut Option<crate::model::VariantId>,
    ) {
        if *last_variant_id != Some(desc.variant_id) {
            // IMPORTANT: for persistent caching with deterministic file-tree layout, the stream key MUST be:
            // `"<master_hash>/<variant_id>"`.
            let new_stream_key: ResourceKey =
                format!("{}/{}", self.master_hash, desc.variant_id.0).into();
            let _ =
                self.data_sender
                    .try_send(StreamMsg::Control(StreamControl::SetDefaultStreamKey {
                        stream_key: new_stream_key,
                    }));
        }
    }

    fn stream_key_for_desc(&self, desc: &crate::manager::SegmentDescriptor) -> ResourceKey {
        // For persistent caching with deterministic file-tree layout, the stream key MUST be:
        // `"<master_hash>/<variant_id>"`.
        format!("{}/{}", self.master_hash, desc.variant_id.0).into()
    }

    fn chunk_kind_for_desc(&self, desc: &crate::manager::SegmentDescriptor) -> ChunkKind {
        if desc.is_init {
            ChunkKind::Init
        } else {
            ChunkKind::Media
        }
    }

    fn filename_hint_for_desc(&self, desc: &crate::manager::SegmentDescriptor) -> Option<Arc<str>> {
        // Filename hint: for now we use the URI basename (query stripped) when possible.
        // This will be used later by file-based storage factories for deterministic naming.
        let uri = desc.uri.as_str();
        let no_query = uri.split('?').next().unwrap_or(uri);
        no_query.rsplit('/').next().map(|s| Arc::<str>::from(s))
    }

    fn cached_segment_key(
        &self,
        stream_key: &ResourceKey,
        filename_hint: &Option<Arc<str>>,
    ) -> Option<ResourceKey> {
        // We address segments by a ResourceKey that is the relative path from storage_root:
        // "<master_hash>/<variant_id>/<segment_basename>".
        filename_hint.as_deref().map(|base| {
            // NOTE: variant_id is encoded into stream_key as "<master_hash>/<variant_id>".
            // We want the full path including the segment basename.
            ResourceKey(format!("{}/{}", stream_key.0, base).into())
        })
    }

    /// Best-effort cache probe.
    /// Returns `Some(len)` on HIT, `None` on MISS / error / disabled.
    fn probe_cached_segment_len(&self, seg_key: &Option<ResourceKey>) -> Option<u64> {
        let Some(seg_key) = seg_key.as_ref() else {
            tracing::trace!("segment cache: disabled for this segment (missing filename_hint)");
            return None;
        };

        match self.storage_handle.len(seg_key) {
            Ok(Some(len)) if len > 0 => {
                tracing::trace!("segment cache: HIT key='{}' ({} bytes)", seg_key.0, len);
                Some(len)
            }
            Ok(Some(_len)) => {
                // Exists but empty: treat as miss to allow refetch.
                tracing::trace!(
                    "segment cache: MISS (empty) key='{}' - treating as miss",
                    seg_key.0
                );
                None
            }
            Ok(None) => {
                tracing::trace!("segment cache: MISS key='{}'", seg_key.0);
                None
            }
            Err(e) => {
                tracing::trace!(
                    "segment cache: READ ERROR key='{}' err='{}' (treating as miss)",
                    seg_key.0,
                    e
                );
                None
            }
        }
    }

    async fn emit_cached_segment_as_chunk(
        &mut self,
        desc: &crate::manager::SegmentDescriptor,
        stream_key: ResourceKey,
        kind: ChunkKind,
        seg_size: Option<u64>,
        filename_hint: Option<Arc<str>>,
        cached_len: u64,
        last_variant_id: &mut Option<crate::model::VariantId>,
    ) -> Result<(), HlsStreamError> {
        // Handle full-segment skip for cached data (media only).
        if self.bytes_to_skip > 0 && !desc.is_init {
            if self.bytes_to_skip >= cached_len {
                self.bytes_to_skip -= cached_len;
                self.current_position += cached_len;
                self.emit_post_segment_events(desc);
                return Ok(());
            }
            // Partial skip within cached segment is not supported yet; fall through and consume it.
            self.bytes_to_skip = 0;
        }

        self.emit_pre_segment_events(last_variant_id, desc, seg_size, Some(cached_len));

        // Update segmented length snapshot for a cached HIT: we "materialize" the
        // segment boundary without downloading bytes.
        {
            let mut guard = self
                .segmented_length
                .write()
                .map_err(|_| HlsStreamError::Cancelled)?;
            guard.segments.push(DynamicLength {
                reported: cached_len,
                gathered: Some(cached_len),
            });
        }

        self.data_sender
            .send(StreamMsg::Control(StreamControl::ChunkStart {
                stream_key: stream_key.clone(),
                kind,
                reported_len: seg_size.or(Some(cached_len)),
                filename_hint: filename_hint.clone(),
            }))
            .await
            .map_err(|_| HlsStreamError::Cancelled)?;

        self.data_sender
            .send(StreamMsg::Control(StreamControl::ChunkEnd {
                stream_key,
                kind,
                gathered_len: cached_len,
            }))
            .await
            .map_err(|_| HlsStreamError::Cancelled)?;

        // Do NOT update ABR on cache HIT: offline-safe behavior.
        self.emit_post_segment_events(desc);

        Ok(())
    }

    async fn open_segment_stream(
        &mut self,
        desc: &crate::manager::SegmentDescriptor,
    ) -> Result<Option<crate::model::HlsByteStream>, HlsStreamError> {
        // Build a stream according to skip and init rules
        let stream_res = if self.bytes_to_skip > 0 && !desc.is_init {
            let start = self.bytes_to_skip;
            self.bytes_to_skip = 0;
            self.controller
                .inner_stream()
                .downloader()
                .stream_segment_range(&desc.uri, start, None)
                .await
        } else {
            // For init segments, we always send the full segment even if skip is inside it
            self.controller
                .inner_stream()
                .downloader()
                .stream_segment(&desc.uri)
                .await
        };

        match stream_res {
            Ok(s) => Ok(Some(s)),
            Err(e) => {
                tracing::error!(
                    "Failed to open segment stream: {}, retrying in {:?}",
                    e,
                    self.retry_delay
                );
                self.backoff_sleep().await?;
                // Retryable failure: caller should continue the loop without treating it as fatal.
                Ok(None)
            }
        }
    }

    fn push_segment_length_on_start(&self, seg_size: Option<u64>) -> Result<(), HlsStreamError> {
        // Update segmented length snapshot (best-effort) on ChunkStart.
        // We append a new segment entry in the same order we emit ChunkStart boundaries.
        let reported = seg_size.unwrap_or(0);
        let mut guard = self
            .segmented_length
            .write()
            .map_err(|_| HlsStreamError::Cancelled)?;
        guard.segments.push(DynamicLength {
            reported,
            gathered: None,
        });
        Ok(())
    }

    fn update_segment_length_on_end(&self, gathered_len: u64) -> Result<(), HlsStreamError> {
        // Update segmented length snapshot (best-effort) on ChunkEnd.
        let mut guard = self
            .segmented_length
            .write()
            .map_err(|_| HlsStreamError::Cancelled)?;
        if let Some(last) = guard.segments.last_mut() {
            last.gathered = Some(gathered_len);
            // If reported was unknown (0), keep reported in sync to avoid 0-length sums.
            if last.reported == 0 {
                last.reported = gathered_len;
            }
        } else {
            // Should not happen (ChunkEnd without ChunkStart), but keep it robust.
            guard.segments.push(DynamicLength {
                reported: gathered_len,
                gathered: Some(gathered_len),
            });
        }
        Ok(())
    }

    /// Process a single segment descriptor: computes size, handles skip, DRM, events and streaming.
    #[instrument(skip(self, last_variant_id))]
    async fn process_descriptor(
        &mut self,
        desc: crate::manager::SegmentDescriptor,
        last_variant_id: &mut Option<crate::model::VariantId>,
    ) -> Result<(), HlsStreamError> {
        // Determine segment size if needed for skip math
        let seg_size = self.compute_segment_size(&desc).await;

        // Handle full-segment skip if we have enough bytes_to_skip and know size
        if self.try_skip_full_segment_by_size(seg_size) {
            return Ok(());
        }

        // Resolve DRM (AES-128-CBC) parameters before emitting events or building middlewares
        let drm_params_opt: Option<([u8; 16], [u8; 16])> =
            self.resolve_drm_params_for_desc(&desc).await;
        let init_len_opt = if desc.is_init || drm_params_opt.is_some() {
            None
        } else {
            seg_size
        };

        self.maybe_update_default_stream_key(&desc, last_variant_id);

        let stream_key = self.stream_key_for_desc(&desc);
        let kind = self.chunk_kind_for_desc(&desc);
        let filename_hint = self.filename_hint_for_desc(&desc);

        // Cache probe before network work.
        let cached_key = self.cached_segment_key(&stream_key, &filename_hint);
        if let Some(cached_len) = self.probe_cached_segment_len(&cached_key) {
            return self
                .emit_cached_segment_as_chunk(
                    &desc,
                    stream_key,
                    kind,
                    seg_size,
                    filename_hint,
                    cached_len,
                    last_variant_id,
                )
                .await;
        }

        self.emit_pre_segment_events(last_variant_id, &desc, seg_size, init_len_opt);

        let Some(stream) = self.open_segment_stream(&desc).await? else {
            // `open_segment_stream` already performed backoff_sleep on failure.
            return Ok(());
        };

        // Update segmented length snapshot (best-effort) on ChunkStart.
        self.push_segment_length_on_start(seg_size)?;

        self.data_sender
            .send(StreamMsg::Control(StreamControl::ChunkStart {
                stream_key: stream_key.clone(),
                kind,
                reported_len: seg_size,
                filename_hint,
            }))
            .await
            .map_err(|_| HlsStreamError::Cancelled)?;

        // Apply streaming middlewares (DRM, etc.) and pump data
        let start_time = std::time::Instant::now();
        let middlewares = HlsStreamWorker::build_middlewares(drm_params_opt);
        let stream = apply_middlewares(stream, &middlewares);
        let gathered_len = self.pump_stream_chunks(stream).await?;
        let elapsed = start_time.elapsed();

        // Report download to ABR (network only). Cache HIT is handled above and intentionally ignored.
        self.controller
            .on_media_segment_downloaded(desc.duration, gathered_len, elapsed);

        self.update_segment_length_on_end(gathered_len)?;

        // Emit ordered control message to finalize the chunk.
        self.data_sender
            .send(StreamMsg::Control(StreamControl::ChunkEnd {
                stream_key,
                kind,
                gathered_len,
            }))
            .await
            .map_err(|_| HlsStreamError::Cancelled)?;

        // Emit segment boundary events after finishing streaming
        self.emit_post_segment_events(&desc);

        Ok(())
    }

    /// Construct a worker by building an `HlsManager` internally (production/default path).
    ///
    /// This is kept for backwards compatibility. Internally it now delegates to
    /// `new_with_manager(...)`, allowing tests/fixtures to inject a pre-built manager.
    pub async fn new(
        url: Url,
        settings: Arc<crate::HlsSettings>,
        storage_handle: StorageHandle,
        data_sender: mpsc::Sender<StreamMsg>,
        seek_receiver: mpsc::Receiver<u64>,
        cancel_token: CancellationToken,
        event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
        master_hash: String,
        segmented_length: Arc<std::sync::RwLock<SegmentedLength>>,
    ) -> Result<Self, HlsStreamError> {
        // Build downloader from flattened settings (for manager)
        let (request_timeout, max_retries, retry_base_delay, max_retry_delay) = (
            settings.request_timeout,
            settings.max_retries,
            settings.retry_base_delay,
            settings.max_retry_delay,
        );

        let manager_downloader = ResourceDownloader::new(
            request_timeout,
            max_retries,
            retry_base_delay,
            max_retry_delay,
        );

        // Read-before-fetch caching for playlists/keys uses the storage handle.
        // Persistence is performed by emitting `StoreResource` via the same ordered stream channel.
        let manager = HlsManager::new(
            url.clone(),
            settings.clone(),
            manager_downloader,
            storage_handle.clone(),
            data_sender.clone(),
        );

        Self::new_with_manager(
            manager,
            storage_handle,
            data_sender,
            seek_receiver,
            cancel_token,
            event_sender,
            master_hash,
            segmented_length,
        )
        .await
    }

    /// Construct a worker from a pre-built `HlsManager`.
    ///
    /// This enables a "matryoshka" style fixture composition:
    /// fixture server -> manager -> worker -> stream -> StreamDownload reader.
    ///
    /// Expectations:
    /// - `manager` must be configured to point at the desired master playlist URL.
    /// - `manager` should be wired to the same `data_sender` if it emits ordered persistence
    ///   controls (e.g. `StreamControl::StoreResource`).
    /// - This method will call `manager.load_master()` as part of initialization (same as `new`).
    pub async fn new_with_manager(
        mut manager: HlsManager,
        storage_handle: StorageHandle,
        data_sender: mpsc::Sender<StreamMsg>,
        seek_receiver: mpsc::Receiver<u64>,
        cancel_token: CancellationToken,
        event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
        master_hash: String,
        segmented_length: Arc<std::sync::RwLock<SegmentedLength>>,
    ) -> Result<Self, HlsStreamError> {
        // Initialize manager and ABR controller
        manager
            .load_master()
            .await
            .map_err(|e| HlsStreamError::Hls(HlsErrorKind::MasterPlaylistLoad(e)))?;

        let master = manager
            .master()
            .ok_or_else(|| HlsStreamError::Hls(HlsErrorKind::StreamingLoopNotInitialized))?;

        let settings = manager.settings().clone();

        let initial_variant_index = {
            if master.variants.is_empty() {
                return Err(HlsStreamError::Hls(HlsErrorKind::NoVariants));
            }

            // If selector returns Some(VariantId) => start in MANUAL mode at that variant.
            // If selector is absent or returns None => start in AUTO mode, using manager default.
            let selected = settings
                .variant_stream_selector
                .as_ref()
                .and_then(|cb| (cb)(master));

            match selected {
                None => manager.current_variant_index().unwrap_or(0),
                Some(id) => id.0,
            }
        };

        let init_bw = master.variants[initial_variant_index]
            .bandwidth
            .unwrap_or(0) as f64;

        let abr_cfg = AbrConfig {
            min_buffer_for_up_switch: settings.abr_min_buffer_for_up_switch,
            down_switch_buffer: settings.abr_down_switch_buffer,
            throughput_safety_factor: settings.abr_throughput_safety_factor,
            up_hysteresis_ratio: settings.abr_up_hysteresis_ratio,
            down_hysteresis_ratio: settings.abr_down_hysteresis_ratio,
            min_switch_interval: settings.abr_min_switch_interval,
        };
        let manual_variant_id = settings
            .variant_stream_selector
            .as_ref()
            .and_then(|cb| (cb)(master));

        let mut controller = AbrController::new(
            manager,
            abr_cfg,
            manual_variant_id,
            initial_variant_index,
            init_bw,
        );

        controller
            .init()
            .await
            .map_err(|e| HlsStreamError::Hls(HlsErrorKind::ControllerInit(e)))?;

        Ok(Self {
            data_sender,
            seek_receiver,
            cancel_token,
            event_sender: event_sender.clone(),
            controller,
            current_position: 0,
            bytes_to_skip: 0,
            retry_delay: Self::INITIAL_RETRY_DELAY,

            storage_handle,
            segmented_length,

            master_hash: Arc::<str>::from(master_hash),
        })
    }

    #[instrument(skip(self))]
    pub async fn run(mut self) -> Result<(), HlsStreamError> {
        let mut last_variant_id: Option<crate::model::VariantId> = None;
        loop {
            // Fetch next segment descriptor using non-blocking method with unified race.
            //
            // Important: we must avoid overlapping mutable borrows of `self.controller`.
            // We do this by letting the controller run ABR logic first, and only then
            // calling into the inner stream to obtain the next descriptor.
            let next_desc = match Self::race_with_seek(
                &self.cancel_token,
                &mut self.seek_receiver,
                self.controller.next_segment_descriptor_nonblocking(),
            )
            .await?
            {
                RaceOutcome::Completed(result) => result,
                RaceOutcome::Seek(position) => {
                    tracing::trace!("HLS streaming loop: received seek to position {}", position);
                    self.apply_seek_position(position).await?;
                    continue;
                }
                RaceOutcome::ChannelClosed => {
                    tracing::trace!("HLS stream: seek channel closed");
                    break;
                }
            };

            match next_desc {
                Ok(crate::manager::NextSegmentDescResult::Segment(desc)) => {
                    // Reset retry delay on success
                    self.retry_delay = Self::INITIAL_RETRY_DELAY;

                    // Delegate per-segment logic to a dedicated helper
                    self.process_descriptor(desc, &mut last_variant_id).await?;
                }
                Ok(crate::manager::NextSegmentDescResult::EndOfStream) => {
                    tracing::trace!("HLS stream: end of stream");
                    break;
                }
                Ok(crate::manager::NextSegmentDescResult::NeedsRefresh { wait }) => {
                    // Live stream needs to wait for new segments
                    // Use select! for proper cancellation during wait
                    // no pre-reserved permit to drop
                    self.handle_needs_refresh(wait).await?;
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to get next segment descriptor: {}, retrying in {:?}",
                        e,
                        self.retry_delay
                    );

                    self.backoff_sleep().await?;
                }
            }
        }

        Ok(())
    }
}
