use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{
    AbrConfig, AbrController, Aes128CbcMiddleware, HlsErrorKind, HlsManager, HlsStreamError,
    MediaStream, ResourceDownloader, StreamEvent, StreamMiddleware, apply_middlewares,
};
use stream_download::source::{ChunkKind, ResourceKey, StreamControl, StreamMsg};

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
}

impl HlsStreamWorker {
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
        self.controller
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
            .flatten()
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
        if let (Some(size), true) = (seg_size, self.bytes_to_skip > 0) {
            if self.bytes_to_skip >= size {
                self.bytes_to_skip -= size;
                self.current_position += size;
                return Ok(());
            }
        }

        // Resolve DRM (AES-128-CBC) parameters before emitting events or building middlewares
        let drm_params_opt = self.resolve_drm_params_for_desc(&desc).await;
        let drm_applied = drm_params_opt.is_some();

        // Emit events around init boundaries and variant changes
        let init_len_opt = if desc.is_init {
            if drm_applied { None } else { seg_size }
        } else {
            None
        };
        // If the variant changed, notify the storage layer so the stitched reader follows the
        // new logical stream key.
        if *last_variant_id != Some(desc.variant_id) {
            let new_stream_key: ResourceKey = format!("hls/variant-{}", desc.variant_id.0).into();
            let _ =
                self.data_sender
                    .try_send(StreamMsg::Control(StreamControl::SetDefaultStreamKey {
                        stream_key: new_stream_key,
                    }));
        }

        self.emit_pre_segment_events(last_variant_id, &desc, seg_size, init_len_opt);

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

        let stream = match stream_res {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    "Failed to open segment stream: {}, retrying in {:?}",
                    e,
                    self.retry_delay
                );
                self.backoff_sleep().await?;
                return Ok(());
            }
        };

        // Emit ordered control message to start a new chunk in segmented storage.
        //
        // We store chunks under a per-variant stream key so different variants/codecs do not mix.
        // The stitched reader follows `SharedState::default_stream_key`, which we can switch via
        // `StreamControl::SetDefaultStreamKey`.
        let stream_key: ResourceKey = format!("hls/variant-{}", desc.variant_id.0).into();
        let kind = if desc.is_init {
            ChunkKind::Init
        } else {
            ChunkKind::Media
        };

        // Filename hint: for now we use the URI basename (query stripped) when possible.
        // This will be used later by file-based storage factories for deterministic naming.
        let filename_hint = {
            let uri = desc.uri.as_str();
            let no_query = uri.split('?').next().unwrap_or(uri);
            no_query.rsplit('/').next().map(|s| Arc::<str>::from(s))
        };

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
        let middlewares = HlsStreamWorker::build_middlewares(drm_params_opt);
        let stream = apply_middlewares(stream, &middlewares);
        let gathered_len = self.pump_stream_chunks(stream).await?;

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

    pub async fn new(
        url: Arc<str>,
        settings: Arc<crate::HlsSettings>,
        data_sender: mpsc::Sender<StreamMsg>,
        seek_receiver: mpsc::Receiver<u64>,
        cancel_token: CancellationToken,
        event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
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

        let mut manager = HlsManager::new(url.clone(), settings.clone(), manager_downloader);

        // Initialize manager and ABR controller
        manager
            .load_master()
            .await
            .map_err(|e| HlsStreamError::Hls(HlsErrorKind::MasterPlaylistLoad(e)))?;

        let master = manager
            .master()
            .ok_or_else(|| HlsStreamError::Hls(HlsErrorKind::StreamingLoopNotInitialized))?;

        let initial_variant_index = {
            if master.variants.is_empty() {
                return Err(HlsStreamError::Hls(HlsErrorKind::NoVariants));
            }
            match settings.selection_manual_variant_id {
                None => manager.current_variant_index().unwrap_or(0),
                Some(index) => index.0,
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
        let manual_variant_id = settings.selection_manual_variant_id;

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
            event_sender,
            controller,
            current_position: 0,
            bytes_to_skip: 0,
            retry_delay: Self::INITIAL_RETRY_DELAY,
        })
    }

    #[instrument(skip(self))]
    pub async fn run(mut self) -> Result<(), HlsStreamError> {
        let mut last_variant_id: Option<crate::model::VariantId> = None;
        loop {
            // Fetch next segment descriptor using non-blocking method with unified race
            let next_desc = match Self::race_with_seek(
                &self.cancel_token,
                &mut self.seek_receiver,
                self.controller
                    .inner_stream_mut()
                    .next_segment_descriptor_nonblocking(),
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
