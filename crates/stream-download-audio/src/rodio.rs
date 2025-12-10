use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use rodio::Source;
use tracing::trace;

use crate::{AudioSpec, AudioStream, FloatSampleSource};

/// Rodio adapter that implements `rodio::Source<Item = f32>` by pulling from an `AudioStream`.
///
/// Notes:
/// - Rodio expects a stable sample rate and channel count during the lifetime of a `Source`.
///   We capture those at construction time from the `AudioStream`.
/// - When the internal buffer is empty, this adapter yields silence to maintain continuity.
///   This avoids busy-wait loops and underruns on the output device.
///
/// Usage:
/// - Create your `AudioStream`.
/// - Wrap it into an `Arc<Mutex<_>>`.
/// - Call `adapt_to_rodio(stream)` to obtain a `rodio::Source<Item=f32>`.
pub struct RodioSourceAdapter {
    inner: Arc<Mutex<AudioStream>>,
    cur_spec: AudioSpec,

    // Background-refilled chunk queue.
    rx: std::sync::mpsc::Receiver<Vec<f32>>,

    // Small pending buffer to amortize locking cost by reading in chunks.
    pending: Vec<f32>,
    cursor: usize,
}

impl RodioSourceAdapter {
    /// Create a new `RodioSourceAdapter` from an `AudioStream`.
    ///
    /// The adapter captures the current output spec (sample rate, channels) at construction.
    pub fn new(inner: Arc<Mutex<AudioStream>>) -> Self {
        let cur_spec = inner.lock().unwrap().spec();

        // Spawn background refill thread which pulls PCM in chunks and sends to a channel.
        let (tx, rx) = std::sync::mpsc::channel::<Vec<f32>>();
        let inner_cloned = Arc::clone(&inner);
        std::thread::spawn(move || {
            let chunk_len = 4096usize;
            loop {
                let mut buf = vec![0.0f32; chunk_len];
                // Pull from AudioStream; if no data right now, back off briefly to avoid busy spin.
                let n = inner_cloned.lock().unwrap().read_interleaved(&mut buf);
                if n > 0 {
                    let _ = tx.send(buf[..n].to_vec());
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            }
        });

        Self {
            inner,
            cur_spec,
            rx,
            pending: Vec::with_capacity(4096),
            cursor: 0,
        }
    }

    fn refill(&mut self) {
        self.pending.clear();
        self.cursor = 0;

        // Non-blocking receive from background thread.
        if let Ok(chunk) = self.rx.try_recv() {
            self.pending = chunk;
        }
    }
}

impl Iterator for RodioSourceAdapter {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.pending.len() {
            // Non-blocking: single attempt to fetch a new chunk.
            self.refill();
        }

        if self.cursor < self.pending.len() {
            let s = self.pending[self.cursor];
            self.cursor += 1;
            Some(s)
        } else {
            // No data available right now; emit silence without blocking.
            Some(0.0)
        }
    }
}

impl Source for RodioSourceAdapter {
    fn current_span_len(&self) -> Option<usize> {
        None
    }

    fn channels(&self) -> u16 {
        self.cur_spec.channels
    }

    fn sample_rate(&self) -> u32 {
        self.cur_spec.sample_rate
    }

    fn total_duration(&self) -> Option<Duration> {
        None
    }
}
