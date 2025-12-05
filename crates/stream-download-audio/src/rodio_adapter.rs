use std::sync::{Arc, Mutex};
use std::time::Duration;

use rodio::Source;

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
        Self {
            inner,
            cur_spec,
            pending: Vec::with_capacity(1024),
            cursor: 0,
        }
    }

    fn refill(&mut self) {
        self.pending.clear();
        self.cursor = 0;

        // Read a chunk from the underlying source.
        // Keep the chunk relatively small to keep latency low.
        let chunk_len = 1024usize;
        let mut buf = vec![0.0f32; chunk_len];
        let n = self.inner.lock().unwrap().read_interleaved(&mut buf);
        if n > 0 {
            self.pending.extend_from_slice(&buf[..n]);
        }
    }
}

impl Iterator for RodioSourceAdapter {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.pending.len() {
            self.refill();
        }

        if self.cursor < self.pending.len() {
            let s = self.pending[self.cursor];
            self.cursor += 1;
            Some(s)
        } else {
            // No data available right now; emit silence to keep the audio clock advancing.
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

/// Helper to adapt an `AudioStream` into a `rodio::Source<Item = f32>`.
pub fn adapt_to_rodio(stream: Arc<Mutex<AudioStream>>) -> RodioSourceAdapter {
    RodioSourceAdapter::new(stream)
}
