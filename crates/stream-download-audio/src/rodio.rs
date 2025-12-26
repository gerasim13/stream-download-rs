//! Rodio adapter for `AudioDecodeStream`.
//!
//! `rodio::Source` is a pull-based, synchronous iterator API. Our decoding pipeline (`AudioDecodeStream`)
//! is an async stream that yields ordered `AudioMsg` items:
//! - `AudioMsg::Pcm(PcmChunk { pcm: Vec<f32>, spec })`
//! - `AudioMsg::Control(AudioControl::...)`
//!
//! This adapter bridges the two by:
//! - spawning a background thread with a small Tokio runtime,
//! - draining `AudioDecodeStream` into a bounded synchronous channel of PCM chunks,
//! - implementing `rodio::Source<Item = f32>` by pulling from that channel.
//!
//! Notes / tradeoffs:
//! - Rodio assumes a fixed output sample rate and channel count for a given `Source`. In practice,
//!   HLS codec switches may still keep the *same* PCM spec. If the spec changes mid-stream, this
//!   adapter will currently terminate playback (by stopping the PCM feed) because rodio cannot
//!   dynamically change format on a single `Source`.
//! - This adapter intentionally focuses on correctness and simplicity for examples.
//!
//! Usage (examples):
//! ```text
//! let audio = AudioDecodeStream::new_hls(...).await?;
//! let source = RodioSourceAdapter::new(audio);
//! sink.append(source);
//! ```

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use rodio::Source;

use crate::{AudioControl, AudioDecodeStream, AudioMsg, AudioSpec};

/// Default buffering for the rodio bridge.
///
/// - `MAX_CHUNKS`: how many PCM chunks we buffer ahead (not samples).
/// - If you want finer control, extend the adapter to accept options.
const MAX_CHUNKS: usize = 32;

/// A bounded synchronous queue of PCM chunks used between:
/// - async decoder thread -> rodio pull thread.
struct PcmQueue {
    inner: Mutex<PcmQueueInner>,
    cv_not_empty: Condvar,
    cv_not_full: Condvar,
}

struct PcmQueueInner {
    q: VecDeque<PcmChunkOwned>,
    closed: bool,
    // Fixed spec once established.
    spec: Option<AudioSpec>,
}

#[derive(Clone)]
struct PcmChunkOwned {
    spec: AudioSpec,
    pcm: Arc<Vec<f32>>,
}

impl PcmQueue {
    fn new() -> Self {
        Self {
            inner: Mutex::new(PcmQueueInner {
                q: VecDeque::new(),
                closed: false,
                spec: None,
            }),
            cv_not_empty: Condvar::new(),
            cv_not_full: Condvar::new(),
        }
    }

    fn close(&self) {
        let mut g = self.inner.lock().unwrap();
        g.closed = true;
        self.cv_not_empty.notify_all();
        self.cv_not_full.notify_all();
    }

    /// Push a PCM chunk (blocking).
    fn push(&self, chunk: PcmChunkOwned) {
        let mut g = self.inner.lock().unwrap();

        // Establish fixed spec on first chunk.
        if g.spec.is_none() {
            g.spec = Some(chunk.spec);
        }

        // If spec changes mid-stream, close the queue to end playback deterministically.
        if g.spec != Some(chunk.spec) {
            g.closed = true;
            self.cv_not_empty.notify_all();
            self.cv_not_full.notify_all();
            return;
        }

        while !g.closed && g.q.len() >= MAX_CHUNKS {
            g = self.cv_not_full.wait(g).unwrap();
        }

        if g.closed {
            return;
        }

        g.q.push_back(chunk);
        self.cv_not_empty.notify_one();
    }

    /// Pop the next PCM chunk (blocking). Returns `None` on EOF/closed.
    fn pop(&self) -> Option<PcmChunkOwned> {
        let mut g = self.inner.lock().unwrap();

        loop {
            if let Some(chunk) = g.q.pop_front() {
                self.cv_not_full.notify_one();
                return Some(chunk);
            }

            if g.closed {
                return None;
            }

            g = self.cv_not_empty.wait(g).unwrap();
        }
    }

    fn spec(&self) -> Option<AudioSpec> {
        let g = self.inner.lock().unwrap();
        g.spec
    }
}

/// Adapter that turns an `AudioDecodeStream` into a `rodio::Source<Item = f32>`.
pub struct RodioSourceAdapter {
    queue: Arc<PcmQueue>,

    // Current chunk being drained.
    cur: Option<PcmChunkOwned>,
    cur_idx: usize,

    // Fixed output spec once known.
    spec: AudioSpec,

    // If true, no more samples will be produced.
    done: bool,
}

impl RodioSourceAdapter {
    /// Create a new `RodioSourceAdapter` and start draining the async stream in a background thread.
    pub fn new(mut stream: AudioDecodeStream) -> Self {
        let queue = Arc::new(PcmQueue::new());
        let queue_bg = queue.clone();

        // Spawn background worker thread with a tiny Tokio runtime.
        thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => {
                    queue_bg.close();
                    return;
                }
            };

            rt.block_on(async move {
                while let Some(msg) = stream.next_msg().await {
                    match msg {
                        AudioMsg::Pcm(chunk) => {
                            // Skip empty chunks (harmless).
                            if chunk.pcm.is_empty() {
                                continue;
                            }
                            queue_bg.push(PcmChunkOwned {
                                spec: chunk.spec,
                                pcm: Arc::new(chunk.pcm),
                            });
                        }
                        AudioMsg::Control(ctrl) => match ctrl {
                            AudioControl::EndOfStream => {
                                break;
                            }
                            // We intentionally ignore boundaries/events here; the audio samples are what matters for rodio.
                            _ => {}
                        },
                    }
                }

                queue_bg.close();
            });
        });

        // Default spec until first PCM arrives. Rodio requires a spec immediately for `Source`.
        // We'll update `spec` after first chunk, but to keep invariants simple, we choose a sane default.
        // The queue will lock the actual spec on first PCM chunk; if it differs, we update lazily.
        let default_spec = AudioSpec {
            sample_rate: 48_000,
            channels: 2,
        };

        Self {
            queue,
            cur: None,
            cur_idx: 0,
            spec: default_spec,
            done: false,
        }
    }

    fn ensure_spec(&mut self) {
        if let Some(s) = self.queue.spec() {
            self.spec = s;
        }
    }
}

impl Iterator for RodioSourceAdapter {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        self.ensure_spec();

        loop {
            // Ensure we have a current chunk.
            if self.cur.is_none() {
                self.cur = self.queue.pop();
                self.cur_idx = 0;

                if self.cur.is_none() {
                    self.done = true;
                    return None;
                }
            }

            let cur = self.cur.as_ref().expect("cur must exist");
            if self.cur_idx < cur.pcm.len() {
                let v = cur.pcm[self.cur_idx];
                self.cur_idx += 1;
                return Some(v);
            } else {
                // Exhausted current chunk, fetch next.
                self.cur = None;
                self.cur_idx = 0;
                continue;
            }
        }
    }
}

impl Source for RodioSourceAdapter {
    fn current_span_len(&self) -> Option<usize> {
        None
    }

    fn channels(&self) -> u16 {
        self.spec.channels
    }

    fn sample_rate(&self) -> u32 {
        self.spec.sample_rate
    }

    fn total_duration(&self) -> Option<std::time::Duration> {
        None
    }
}
