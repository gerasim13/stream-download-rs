//! A bounded byte queue that can be written from async tasks and read from a blocking decoder.
//!
//! Why this exists
//! ---------------
//! Symphonia expects a blocking `Read` interface (and often performs probing/packet reads on a
//! blocking thread). Our network/HLS sources are async and produce `Bytes` chunks.
//!
//! This queue bridges the two worlds with **real backpressure**:
//! - Writers are blocked when buffered bytes reach `max_buffered_bytes`.
//! - Readers are blocked when the queue is empty.
//! - EOF is explicit via `close()`.
//!
//! Importantly, this avoids any busy-wait/sleep polling in `Read::read`.
//!
//! Design notes
//! ------------
//! - This module intentionally uses `std::sync` primitives; the reader runs on a blocking thread.
//! - This is *not* a ring buffer; it stores `bytes::Bytes` chunks and slices them as needed.
//! - Cloning the writer is supported.
//! - There is a single logical reader (SPSC-ish), but multiple writers are fine.

use std::collections::VecDeque;
use std::io;
use std::io::Read;
use std::sync::{Arc, Condvar, Mutex};

use bytes::Bytes;

/// Shared state guarded by a mutex.
#[derive(Debug)]
struct Inner {
    /// Queue of chunks in FIFO order.
    queue: VecDeque<Bytes>,
    /// Offset into the front chunk (how many bytes already consumed).
    front_off: usize,
    /// Total number of bytes currently buffered (across all chunks, excluding consumed part).
    buffered: usize,
    /// True once the producer has indicated EOF.
    closed: bool,
    /// True once a fatal error has been set.
    error: Option<io::Error>,
}

impl Inner {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            front_off: 0,
            buffered: 0,
            closed: false,
            error: None,
        }
    }

    fn pop_front_if_fully_consumed(&mut self) {
        if let Some(front) = self.queue.front() {
            if self.front_off >= front.len() {
                self.queue.pop_front();
                self.front_off = 0;
            }
        }
    }
}

/// A bounded byte queue.
///
/// Construct via [`ByteQueue::new`], then use:
/// - [`ByteQueue::writer`] to get a clonable writer handle (used by async source loop),
/// - [`ByteQueue::reader`] to get a blocking reader implementing `Read` (used by decoder thread).
#[derive(Debug, Clone)]
pub struct ByteQueue {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    max_buffered_bytes: usize,
    mu: Mutex<Inner>,
    cv_not_empty: Condvar,
    cv_not_full: Condvar,
}

impl ByteQueue {
    /// Create a new bounded byte queue.
    ///
    /// `max_buffered_bytes` must be > 0.
    pub fn new(max_buffered_bytes: usize) -> Self {
        assert!(max_buffered_bytes > 0, "max_buffered_bytes must be > 0");
        Self {
            shared: Arc::new(Shared {
                max_buffered_bytes,
                mu: Mutex::new(Inner::new()),
                cv_not_empty: Condvar::new(),
                cv_not_full: Condvar::new(),
            }),
        }
    }

    /// Get a clonable writer handle.
    pub fn writer(&self) -> ByteQueueWriter {
        ByteQueueWriter {
            shared: self.shared.clone(),
        }
    }

    /// Get a blocking reader handle.
    ///
    /// Note: you should generally create exactly one reader and move it into the decoder thread.
    pub fn reader(&self) -> ByteQueueReader {
        ByteQueueReader {
            shared: self.shared.clone(),
        }
    }
}

/// A clonable writer handle for [`ByteQueue`].
#[derive(Debug, Clone)]
pub struct ByteQueueWriter {
    shared: Arc<Shared>,
}

impl ByteQueueWriter {
    /// Push a chunk of bytes into the queue.
    ///
    /// This method blocks if the queue is full (buffered >= max_buffered_bytes).
    ///
    /// Returns:
    /// - `Ok(())` on success
    /// - `Err` if the queue is closed or an error was set
    pub fn push(&self, bytes: Bytes) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }

        // If a single chunk is larger than max capacity, we still allow it (otherwise we'd deadlock),
        // but it will consume all capacity and block subsequent writers until the reader drains it.
        loop {
            let mut guard = self.shared.mu.lock().unwrap();

            if let Some(err) = guard.error.as_ref() {
                return Err(io::Error::new(err.kind(), err.to_string()));
            }
            if guard.closed {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "byte queue is closed",
                ));
            }

            let cap = self.shared.max_buffered_bytes;
            let full = guard.buffered >= cap;

            if full && guard.buffered > 0 {
                // Wait until there's room.
                drop(guard);
                let mut guard = self.shared.mu.lock().unwrap();
                guard = self.shared.cv_not_full.wait(guard).unwrap();
                drop(guard);
                continue;
            }

            // We have space (or buffer is empty but chunk may exceed cap and we allow it).
            guard.buffered = guard.buffered.saturating_add(bytes.len());
            guard.queue.push_back(bytes);
            // wake reader
            self.shared.cv_not_empty.notify_one();
            return Ok(());
        }
    }

    /// Close the queue (EOF).
    pub fn close(&self) {
        let mut guard = self.shared.mu.lock().unwrap();
        guard.closed = true;
        self.shared.cv_not_empty.notify_all();
        self.shared.cv_not_full.notify_all();
    }
}

/// A blocking reader handle for [`ByteQueue`].
#[derive(Debug)]
pub struct ByteQueueReader {
    shared: Arc<Shared>,
}

impl Read for ByteQueueReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }

        loop {
            let mut guard = self.shared.mu.lock().unwrap();

            if let Some(err) = guard.error.take() {
                // Ensure any waiters wake.
                self.shared.cv_not_empty.notify_all();
                self.shared.cv_not_full.notify_all();
                return Err(err);
            }

            guard.pop_front_if_fully_consumed();

            if let Some(front) = guard.queue.front() {
                // We have bytes to read.
                let start = guard.front_off.min(front.len());
                let avail = front.len().saturating_sub(start);
                if avail == 0 {
                    // Front was fully consumed; loop again to pop it.
                    guard.pop_front_if_fully_consumed();
                    continue;
                }

                let n = avail.min(out.len());
                out[..n].copy_from_slice(&front.slice(start..start + n));

                guard.front_off = guard.front_off.saturating_add(n);
                guard.buffered = guard.buffered.saturating_sub(n);

                // If we freed space, wake writers.
                self.shared.cv_not_full.notify_one();
                return Ok(n);
            }

            // No buffered bytes.
            if guard.closed {
                // EOF
                return Ok(0);
            }

            // Wait for data or close/error.
            drop(guard);
            let mut guard = self.shared.mu.lock().unwrap();
            guard = self.shared.cv_not_empty.wait(guard).unwrap();
            drop(guard);
        }
    }
}
