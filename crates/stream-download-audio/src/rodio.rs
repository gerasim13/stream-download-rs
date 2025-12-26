//! Rodio integration for `stream-download-audio`.
//!
//! This adapter is intentionally built on top of the **new ordered** `AudioMsg` stream API.
//! There is no legacy pull-based wrapper.
//!
//! Design
//! ------
//! - Owns an `AudioStream<P>` and spawns a background thread.
//! - The thread continuously pulls ordered `AudioMsg` items from the stream and forwards PCM to
//!   the rodio `Source` iterator via a bounded channel (backpressure).
//! - The iterator (`RodioSourceAdapter`) never blocks in `next()`; if no PCM is available yet it
//!   yields silence (0.0) to keep rodio’s mixer running smoothly.
//!
//! Notes
//! -----
//! - Rodio expects a stable sample rate and channel count. We capture these from the first
//!   `AudioFrame` observed and then keep them constant for the lifetime of the adapter.
//! - If the audio stream reports `AudioControl::EndOfStream`, the adapter will eventually stop
//!   producing samples (returns `None`), which allows `sink.sleep_until_end()` to complete.
//!
//! Requirements
//! ------------
//! This module is feature-gated behind `stream-download-audio`'s `rodio` feature.

#![cfg(feature = "rodio")]

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rodio::Source;

use crate::api::{AudioControl, AudioMsg, AudioSpec};
use crate::{AudioStream, StorageProvider};

/// How the adapter should behave when no PCM is immediately available.
///
/// Rodio's `Source` is a synchronous iterator. If we block there, we can stall the mixer.
/// The default behavior is to yield silence when empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyBehavior {
    /// Yield zeros when no PCM is ready.
    YieldSilence,
    /// Sleep briefly and try again (still yields silence if nothing arrives).
    ///
    /// This reduces busy looping at the cost of slightly higher latency.
    SleepAndYieldSilence(Duration),
}

impl Default for EmptyBehavior {
    fn default() -> Self {
        Self::SleepAndYieldSilence(Duration::from_millis(2))
    }
}

/// A `rodio::Source<Item = f32>` backed by an `AudioStream<P>`.
///
/// This adapter converts the ordered `AudioMsg` stream into interleaved `f32` samples for rodio.
pub struct RodioSourceAdapter<P: StorageProvider> {
    // We keep spec stable for rodio. It's filled after we observe the first frame.
    spec: AudioSpec,

    // Pending PCM interleaved samples for Iterator::next()
    pending: Vec<f32>,
    cursor: usize,

    // Channel fed by background thread.
    rx: mpsc::Receiver<Vec<f32>>,

    // End-of-stream flag set by background thread.
    eof: Arc<std::sync::atomic::AtomicBool>,

    // Behavior when empty
    empty_behavior: EmptyBehavior,

    // Keep the thread handle alive for the lifetime of the adapter.
    _thread: Option<std::thread::JoinHandle<()>>,

    // We keep the stream in an Arc/Mutex only to move it into a thread easily and to allow future
    // extensions. Access happens only inside the spawned thread currently.
    _inner: Arc<Mutex<AudioStream<P>>>,
}

impl<P: StorageProvider + 'static> RodioSourceAdapter<P> {
    /// Create a new `RodioSourceAdapter` from an `AudioStream`.
    ///
    /// This spawns a background thread that pulls `AudioMsg` from the stream and forwards PCM to
    /// the adapter.
    ///
    /// # Panics
    /// Panics if the adapter can't determine an initial `AudioSpec` within the timeout.
    /// (This should not happen for normal streams.)
    pub fn new(stream: AudioStream<P>) -> Self {
        Self::new_with_behavior(stream, EmptyBehavior::default())
    }

    /// Create a new adapter with a custom empty-buffer behavior.
    pub fn new_with_behavior(stream: AudioStream<P>, empty_behavior: EmptyBehavior) -> Self {
        // Bounded queue to prevent unbounded growth if rodio is slow.
        // Each message is a batch of interleaved PCM samples.
        let (tx, rx) = mpsc::sync_channel::<Vec<f32>>(16);

        let inner = Arc::new(Mutex::new(stream));

        // Determine initial spec by pulling the first audio frame on the background thread,
        // then sending it through a one-shot channel.
        let (spec_tx, spec_rx) = mpsc::sync_channel::<AudioSpec>(1);

        let eof = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let eof_thread = eof.clone();
        let inner_thread = inner.clone();

        let thread = std::thread::spawn(move || {
            // Create a tokio runtime in this thread so we can await the async `AudioStream`.
            // (Rodio `Source` is sync; we bridge via this runtime + channel.)
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!("RodioSourceAdapter: failed to build tokio runtime: {}", e);
                    eof_thread.store(true, std::sync::atomic::Ordering::Relaxed);
                    return;
                }
            };

            rt.block_on(async move {
                // Pull messages from the audio stream and forward PCM batches to rodio.
                // We also capture the first `AudioSpec` we see.
                let mut sent_spec = false;

                // We need to own a mutable reference to the stream to call `.next().await`.
                // The stream itself implements `Stream<Item = AudioMsg>`.
                let mut stream_guard = inner_thread
                    .lock()
                    .expect("RodioSourceAdapter: inner stream mutex poisoned");

                loop {
                    // Use the dedicated async API; it does not require `AudioStream<P>: Unpin`.
                    let msg = stream_guard.next_msg().await;
                    match msg {
                        Some(AudioMsg::Frame(frame)) => {
                            if !sent_spec {
                                // First frame determines rodio output spec.
                                let _ = spec_tx.send(frame.spec);
                                sent_spec = true;
                            }

                            // Backpressure: block when rodio is behind.
                            if tx.send(frame.pcm).is_err() {
                                // Consumer dropped; stop thread.
                                tracing::info!(
                                    "RodioSourceAdapter: PCM consumer dropped; stopping"
                                );
                                break;
                            }
                        }
                        Some(AudioMsg::Control(AudioControl::FormatChanged { spec })) => {
                            // Rodio requires stable spec; ignore further format changes.
                            // We still want to set initial spec if we haven't yet.
                            if !sent_spec {
                                let _ = spec_tx.send(spec);
                                sent_spec = true;
                            }
                        }
                        Some(AudioMsg::Control(AudioControl::EndOfStream)) => {
                            eof_thread.store(true, std::sync::atomic::Ordering::Relaxed);
                            break;
                        }
                        None => {
                            // Stream ended/dropped.
                            eof_thread.store(true, std::sync::atomic::Ordering::Relaxed);
                            break;
                        }
                    }
                }
            });
        });

        // Wait for initial spec to be reported.
        // Keep this small; if stream can't produce spec quickly it likely won't play anyway.
        let spec = match spec_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(spec) => spec,
            Err(_) => {
                // Default fallback; but per our contract we panic because rodio needs stable spec.
                // If you want a non-panicking behavior, change this to a Result-returning ctor.
                panic!("RodioSourceAdapter: failed to determine initial AudioSpec from stream");
            }
        };

        Self {
            spec,
            pending: Vec::with_capacity(4096),
            cursor: 0,
            rx,
            eof,
            empty_behavior,
            _thread: Some(thread),
            _inner: inner,
        }
    }

    fn refill(&mut self) {
        self.pending.clear();
        self.cursor = 0;

        match self.rx.try_recv() {
            Ok(chunk) => {
                self.pending = chunk;
            }
            Err(mpsc::TryRecvError::Empty) => {
                // Nothing available right now.
                match self.empty_behavior {
                    EmptyBehavior::YieldSilence => {}
                    EmptyBehavior::SleepAndYieldSilence(d) => std::thread::sleep(d),
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                // Producer stopped; we'll check eof in `next()`.
            }
        }
    }
}

impl<P: StorageProvider + 'static> Iterator for RodioSourceAdapter<P> {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.pending.len() {
            self.refill();
        }

        if self.cursor < self.pending.len() {
            let s = self.pending[self.cursor];
            self.cursor += 1;
            return Some(s);
        }

        // No buffered PCM right now.
        if self.eof.load(std::sync::atomic::Ordering::Relaxed) {
            // End-of-stream: stop producing samples.
            None
        } else {
            // Keep rodio alive by yielding silence.
            Some(0.0)
        }
    }
}

impl<P: StorageProvider + 'static> Source for RodioSourceAdapter<P> {
    fn current_span_len(&self) -> Option<usize> {
        None
    }

    fn channels(&self) -> u16 {
        self.spec.channels
    }

    fn sample_rate(&self) -> u32 {
        self.spec.sample_rate
    }

    fn total_duration(&self) -> Option<Duration> {
        None
    }
}
