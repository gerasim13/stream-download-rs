use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{
    AbrConfig, AbrController, Aes128CbcMiddleware, HlsErrorKind, HlsManager, HlsStreamError,
    MediaStream, ResourceDownloader, StreamEvent, StreamMiddleware, apply_middlewares,
};

pub struct HlsStreamWorker {
    data_sender: mpsc::Sender<Bytes>,
    seek_receiver: mpsc::Receiver<u64>,
    cancel_token: CancellationToken,
    event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
    streaming_downloader: ResourceDownloader,
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
        tokio::select! {
            biased;
            _ = self.cancel_token.cancelled() => {
                return Err(HlsStreamError::Cancelled);
            }
            seek = self.seek_receiver.recv() => {
                if let Some(position) = seek {
                    tracing::trace!("HLS streaming loop: received seek during wait");
                    self.apply_seek_position(position).await?;
                }
                // Continue regardless
            }
            _ = tokio::time::sleep(wait) => {
                tracing::trace!("HLS stream: live refresh wait completed");
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

    /// Pump bytes from a segment stream to downstream with backpressure, cancellation and seek handling.
    #[instrument(skip(self, stream))]
    async fn pump_stream_chunks(
        &mut self,
        mut stream: crate::HlsByteStream,
    ) -> Result<(), HlsStreamError> {
        // Reserve on demand to avoid long-lived borrows during prefetch phases
        let mut first_permit: Option<tokio::sync::mpsc::Permit<Bytes>> = None;

        loop {
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    return Err(HlsStreamError::Cancelled);
                }
                // Concurrently react to a seek during streaming a segment
                seek = self.seek_receiver.recv() => {
                    if let Some(position) = seek {
                        tracing::trace!("HLS streaming loop: received seek during streaming");
                        self.apply_seek_position(position).await?;
                        // break to apply new position on next iteration
                        break;
                    } else {
                        tracing::trace!("HLS stream: seek channel closed");
                        break;
                    }
                }
                item = futures_util::StreamExt::next(&mut stream) => {
                    match item {
                        Some(Ok(chunk)) => {
                            // Send chunk
                            let permit = match first_permit.take() {
                                Some(p) => p,
                                None => match self.data_sender.reserve().await {
                                    Ok(p) => p,
                                    Err(_) => { tracing::trace!("HLS stream: receiver dropped, stopping"); break; }
                                },
                            };
                            let len = chunk.len() as u64;
                            permit.send(chunk);
                            self.current_position = self.current_position.saturating_add(len);
                        }
                        Some(Err(e)) => {
                            tracing::error!("Error reading segment chunk: {}, retrying in {:?}", e, self.retry_delay);
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
        self.emit_pre_segment_events(last_variant_id, &desc, seg_size, init_len_opt);

        // Build a stream according to skip and init rules
        let stream_res = if self.bytes_to_skip > 0 && !desc.is_init {
            let start = self.bytes_to_skip;
            self.bytes_to_skip = 0;
            self.streaming_downloader
                .stream_segment_range(&desc.uri, start, None)
                .await
        } else {
            // For init segments, we always send the full segment even if skip is inside it
            self.streaming_downloader.stream_segment(&desc.uri).await
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

        // Apply streaming middlewares (DRM, etc.) and pump data
        let middlewares = HlsStreamWorker::build_middlewares(drm_params_opt);
        let stream = apply_middlewares(stream, &middlewares);
        self.pump_stream_chunks(stream).await?;

        // Emit segment boundary events after finishing streaming
        self.emit_post_segment_events(&desc);

        Ok(())
    }

    pub async fn new(
        url: Arc<str>,
        settings: Arc<crate::HlsSettings>,
        data_sender: mpsc::Sender<Bytes>,
        seek_receiver: mpsc::Receiver<u64>,
        cancel_token: CancellationToken,
        event_sender: tokio::sync::broadcast::Sender<StreamEvent>,
    ) -> Result<Self, HlsStreamError> {
        // Build dedicated downloaders from flattened settings
        let (request_timeout, max_retries, retry_base_delay, max_retry_delay) = (
            settings.request_timeout,
            settings.max_retries,
            settings.retry_base_delay,
            settings.max_retry_delay,
        );

        let streaming_downloader = ResourceDownloader::new(
            request_timeout,
            max_retries,
            retry_base_delay,
            max_retry_delay,
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
            streaming_downloader,
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
            // Fetch next segment descriptor using non-blocking method
            let next_desc = tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    return Err(HlsStreamError::Cancelled);
                }
                seek = self.seek_receiver.recv() => {
                    if let Some(position) = seek {
                        tracing::trace!("HLS streaming loop: received seek to position {}", position);
                        self.apply_seek_position(position).await?;
                        // no pre-reserved permit; continue
                        continue;
                    } else {
                        tracing::trace!("HLS stream: seek channel closed");
                        break;
                    }
                }
                result = self.controller.inner_stream_mut().next_segment_descriptor_nonblocking() => result,
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
