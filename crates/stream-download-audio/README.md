# stream-download-audio

Trait-based audio pipeline for the `stream-download` workspace: decode HTTP or HLS into interleaved `f32` PCM with **codec-switch-safe** behavior, **bounded backpressure** (producer waits, consumer is non-blocking), and an optional **rodio adapter**.

This README is a **specification of the public API and architecture**. It intentionally avoids implementation details and internal module structure.

## Goals

- Provide an audio layer comparable in spirit to AVPlayer:
  - HTTP single-file playback
  - HLS adaptive streaming (ABR/manual switching)
  - Seamless codec changes (AAC ↔ FLAC) via decoder reinitialization
- Expose audio as **interleaved `f32` PCM** with predictable buffering semantics.
- Support **seek by time** (`Duration`) even though underlying streams are `Read + Seek` by bytes.
- Allow deterministic testing: consume an async `Stream` of PCM chunks, issue commands (seek / HLS variant switch), and validate output.

## Non-goals (for v1 of this refactor)

- Automatic sample-rate conversion / resampling (rubato integration planned).
- Complex DSP/effects chain (a processing interface exists, but wiring is deferred).
- Real-time audio callback guarantees. The consumer must not block; the producer may block.

---

## High-level Design

You build a single high-level component:

- Generic over:
  - `S`: the underlying stream type (`HlsStream` or `HttpStream` or other compatible stream in the future)
  - `P`: the `StorageProvider` used by `stream-download` (and by `stream-download-hls` storage/caching)

Internally it owns a **trait-based audio source** that:
1. owns/uses `StreamDownload<S, P>` (with `Settings<S>` provided at initialization)
2. decodes via Symphonia into PCM frames
3. (optionally in future) processes PCM (DSP/resampling)
4. sends PCM chunks to a bounded channel

### Critical buffering semantics

- The pipeline uses a bounded producer/consumer queue for PCM chunks.
- The **producer waits** when the queue is full (ensures *no sample loss* and preserves alignment).
- The **consumer is non-blocking**: when no chunk is available, the async stream yields `Pending` (never forces the caller to wait).

This is the core safety property for playback quality: no random dropping of samples and no channel misalignment.

---

## Public API: Concepts and Contracts

### PCM types

- All PCM delivered to consumers is:
  - interleaved
  - `f32`
  - frame-aligned (`len % channels == 0`)

```/dev/null/stream_download_audio_spec.md#L1-60
pub struct AudioSpec {
    pub sample_rate: u32,
    pub channels: u16,
}

pub struct PcmChunk {
    pub spec: AudioSpec,
    pub pcm: Vec<f32>, // interleaved, length is multiple of channels
}
```

### Commands (seek + HLS-only variant switching)

The high-level component exposes a command channel:

- `Seek(Duration)` is supported by all sources.
- `SetHlsVariant` is supported only by HLS sources:
  - HTTP sources ignore or return a not-supported error (implementation choice), but the API exists.

```/dev/null/stream_download_audio_spec.md#L62-90
pub enum AudioCommand {
    Seek(std::time::Duration),

    /// Manual variant switch (HLS only).
    SetHlsVariant { variant: u64 },

    /// Return to ABR/auto selection (HLS only).
    ClearHlsVariantOverride,
}
```

### Seeking

- The user-facing seek API is **time-based**: `seek(Duration)`.
- Under the hood:
  - underlying streams support `Read + Seek` by bytes
  - Symphonia supports seeks by time via the `FormatReader`
- **HTTP source**: creates `FormatReader/Decoder` once and seeks within the format reader.
- **HLS source**: recreates `FormatReader/Decoder` on init/variant epoch changes, but always on top of the same `StreamDownload` reader.

Sample-rate changes are included in `PcmChunk.spec`. In v1, behavior on sample-rate conversion is deferred; the pipeline should call an internal placeholder hook (`todo`) where resampling will later be integrated.

```/dev/null/stream_download_audio_spec.md#L92-150
pub trait AudioSource: Send {
    fn output_spec(&self) -> Option<AudioSpec>;

    fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError>;

    fn handle_command(&mut self, cmd: AudioCommand) -> Result<(), AudioError>;
}
```

Notes:
- `AudioSource` is intentionally synchronous; the high-level pipeline runs it in a worker task.
- `handle_command` is best-effort. For HTTP sources, HLS-specific commands may be ignored or reported as not supported.

---

## The High-level Component (generic over Stream + Storage)

### Generic parameters

The high-level component is generic over:
- `S`: the stream implementation (`HlsStream` / `HttpStream`)
- `P`: the storage provider type used to build and operate the stream

This keeps integration with the `stream-download` ecosystem explicit and testable.

```/dev/null/stream_download_audio_spec.md#L152-235
pub struct AudioStream<S, P> {
    // Owns a trait-based source which itself owns/uses the stream + storage.
    // source: Box<dyn AudioSource>,

    // Producer/consumer queue for PCM chunks.
    // Producer waits when full; consumer is polled (non-blocking).
}

impl<S, P> AudioStream<S, P> {
    /// Spawns the decode/produce worker and returns a non-blocking async stream of PCM chunks.
    ///
    /// `settings` is the `stream-download` settings for the underlying stream type `S`.
    pub async fn new(source: Box<dyn AudioSource>, settings: AudioSettings) -> Result<Self, AudioError>;

    pub fn output_spec(&self) -> Option<AudioSpec>;

    /// Send a command to the worker/source.
    pub fn command_tx(&self) -> tokio::sync::mpsc::Sender<AudioCommand>;
}

impl<S, P> futures_util::Stream for AudioStream<S, P> {
    type Item = Result<PcmChunk, AudioError>;
}
```

Contract:
- When no data is available, `poll_next` returns `Pending`.
- On end-of-stream, the stream returns `Ready(None)`.
- On fatal error, the stream yields `Some(Err(..))` and then terminates.

### Worker model

- The pipeline runs a worker which repeatedly:
  - calls `source.next_chunk()`
  - sends produced `PcmChunk`s into the producer side of the bounded channel
- The send operation **blocks** when the channel is full.
- This guarantees:
  - no silent drops
  - stable chunk ordering

Implementation may use:
- a dedicated OS thread
- or a synchronous task within a Tokio runtime
- or an async task + spawn-blocking bridge

The contract is the same regardless of runtime strategy.

---

## Settings

Settings configure buffering and target format constraints.

```/dev/null/stream_download_audio_spec.md#L207-240
pub struct AudioSettings {
    /// Capacity in frames for the producer/consumer buffer.
    /// Larger values increase latency but reduce risk of underrun.
    pub ring_capacity_frames: usize,

    /// Target channel count (v1: typically 2).
    pub target_channels: u16,

    /// Target sample rate.
    /// In v1 this is a constraint/expectation; resampling is planned.
    pub target_sample_rate: u32,
}
```

Notes:
- v1 focuses on correct decode + correctness under variant changes.
- resampling and automatic sample-rate conversion are planned (rubato).

---

## Rodio Adapter (optional feature)

With feature `rodio`, the crate provides an adapter that exposes `AudioStream` as a `rodio::Source<Item = f32>`.

Constraints:
- rodio pulls samples synchronously.
- the adapter is expected to drive the async `AudioStream` in a background task and buffer PCM internally.
- chunk sizes are arbitrary; the adapter must keep a remainder buffer for partial consumption.

```/dev/null/stream_download_audio_spec.md#L260-310
// feature = "rodio"
pub struct RodioSourceAdapter { /* ... */ }

// Implements `rodio::Source<Item = f32>`
```

---

## Testing philosophy

The design explicitly supports deterministic tests:

- Obtain an iterator over `PcmChunk`s via `chunks()`.
- Read N chunks, perform `seek(Duration)`, then read more.
- Validate:
  - chunk sizes are frame-aligned
  - output spec is consistent (or changes only when expected)
  - PCM after seek corresponds to the new position

For HLS:
- tests should prefer **manual variant switching** for determinism.
- ABR behavior is heuristic; if tested, it should be driven by controlled network shaping rather than timing assumptions.

---

## Backends

### HTTP

- Single resource.
- Decoder is created once and read continuously.
- Seek uses Symphonia time-based seek within the format reader.

### HLS

- Segmented content with potential codec changes across variants.
- Decoder must be recreated when variant/init epoch changes.
- Seek must be designed to remain correct across decoder recreation.

---

## Status

This README describes the new trait-based design and contracts.

Next steps (implementation):
- define the public types and traits
- implement HTTP source
- implement HLS source with decoder-epoch handling
- implement bounded producer/consumer channel semantics
- implement rodio adapter
- add deterministic integration tests (HTTP + HLS real assets)

---

## License

Dual-licensed under MIT or Apache-2.0, at your option. See the workspace licenses for details.