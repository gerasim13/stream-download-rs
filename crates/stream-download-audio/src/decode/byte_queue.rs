//! Legacy byte queue (removed).
//!
//! This module previously implemented a bounded, blocking byte queue bridging async producers
//! to a blocking Symphonia decoder via `Read`.
//!
//! The audio pipeline has since been refactored to use **bounded async byte channels per epoch**
//! (see `decode::symphonia_decoder::{EpochMsg::StartEpochFromAsyncBytes, AsyncBytesMediaSource}`),
//! which provides backpressure without blocking async tasks.
//!
//! As a result, the old byte-queue implementation is no longer used and has been intentionally
//! deleted to keep the crate warning-free and to avoid maintaining dead code.
#![allow(dead_code)]
