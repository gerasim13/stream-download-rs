# stream-download-audio

Trait-based, production-oriented audio pipeline for the `stream-download` workspace: decode HTTP or HLS into interleaved `f32` PCM with **codec-switch-safe** behavior, **blocking backpressure** on the producer side, and an optional **rodio adapter**.

This README is a **specification of the public API and architecture**. It intentionally avoids implementation details and internal module structure.

## Goals

- Provide an audio layer comparable in spirit to AVPlayer:
  - HTTP single-file playback
  - HLS adaptive streaming (ABR/manual switching)
  - Seamless codec changes (AAC ↔ FLAC) via decoder reinitialization
- Expose audio as **interleaved `f32` PCM** with predictable buffering semantics.
- Support **seek by time** (`Duration`) even though underlying streams are `Read + Seek` by bytes.
- Allow deterministic testing: a consumer can iterate over sample chunks, issue seeks, and validate output.

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
1. reads bytes from `S` (`Read + Seek`)
2. decodes via Symphonia into PCM frames
3. (optionally in future) processes PCM (DSP/resampling)
4. sends PCM chunks to a bounded channel

### Critical buffering semantics

- The pipeline uses a **producer/consumer channel** for PCM chunks.
- **Producer blocks** when the channel is full (ensures *no sample loss* and preserves alignment).
- **Consumer never blocks** (it is polled from an audio thread).

This is the core safety property for playback quality: no random dropping of samples and no channel misalignment.

---

## Public API: Concepts and Contracts

### PCM types

- All PCM delivered to consumers is:
  - interleaved
  - `f32`
  - frame-aligned (`len % channels == 0`)

```/dev/null/stream_download_audio_spec.md#L1-40
pub struct AudioSpec {
    pub sample_rate: u32,
    pub channels: u16,
}

pub struct PcmChunk {
    pub spec: AudioSpec,
    pub pcm: Vec<f32>, // interleaved, length is multiple of channels
}
```

### Seeking

- The user-facing seek API is **time-based**: `seek(Duration)`.
- Under the hood:
  - underlying streams support `Read + Seek` by bytes
  - Symphonia supports accurate seeks by time via the `FormatReader`
- **HTTP source**: creates a decoder once and seeks within the format reader.
- **HLS source**: must **recreate the decoder** when variant changes; seeking must therefore be designed to work across decoder recreation.

Seek is **best-effort**:
- if seek cannot be performed, the pipeline reports it (via a control message / error, see below) and continues from a sensible state.

### HLS variant switching

- Variant switches may change codecs and sample formats.
- On HLS variant change, the source must:
  - reset decoder state
  - reinitialize the decoder on the new init segment/epoch
  - continue emitting PCM chunks without corrupting output

---

## Trait-based Sources

The high-level component holds a boxed trait object representing the active source.

The intent is that:
- `HttpAudioSource` is simple: single decoder lifetime, regular reads/seeks.
- `HlsAudioSource` is more complex: decoder epochs and reinitialization on variant/init changes.

### Source responsibilities

A source is responsible for:
- pulling bytes from its underlying `S: Read + Seek`
- decoding bytes to PCM (`f32`, interleaved)
- (later) applying processing/resampling
- producing `PcmChunk`s to the pipeline

The pipeline does **not** want to know whether bytes come from HTTP, HLS, cache, etc.

```/dev/null/stream_download_audio_spec.md#L42-120
pub trait AudioSource: Send {
    /// Returns the current output spec, if known.
    /// The spec becomes known after decoder initialization.
    fn output_spec(&self) -> Option<AudioSpec>;

    /// Decode and produce the next chunk of PCM.
    ///
    /// - Returns `Ok(Some(chunk))` when PCM is produced.
    /// - Returns `Ok(None)` when the source reached end-of-stream.
    /// - Returns `Err` on unrecoverable decode/source failures.
    fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError>;

    /// Best-effort seek by time.
    ///
    /// Implementations should use Symphonia time-based seeking when possible.
    /// For HLS, this may recreate decoder state as needed.
    fn seek(&mut self, position: std::time::Duration) -> Result<(), AudioError>;
}
```

> Note: exact error typing is not fixed by this README; the contract is that errors are explicit and never silently drop samples.

---

## The High-level Component (generic over Stream + Storage)

### Generic parameters

The high-level component is generic over:
- `S`: the stream implementation (`HlsStream` / `HttpStream`)
- `P`: the storage provider type used to build and operate the stream

This keeps integration with the `stream-download` ecosystem explicit and testable.

```/dev/null/stream_download_audio_spec.md#L122-205
pub struct AudioPipeline<S, P> {
    // Owns a trait-based source which itself owns/uses the stream + storage.
    // source: Box<dyn AudioSource>,

    // Producer/consumer channel for PCM chunks.
    // Producer side blocks when full; consumer side is non-blocking.
}

impl<S, P> AudioPipeline<S, P> {
    /// Spawn the decode/produce worker and return a handle that the audio thread can pull from.
    pub fn new(stream: S, storage: P, settings: AudioSettings) -> Self;

    /// Non-blocking consumer: attempts to pop a chunk.
    /// Returns `None` if no chunk is currently available.
    pub fn try_next_chunk(&mut self) -> Option<PcmChunk>;

    /// For tests: blocking-ish iterator that yields chunks (may block internally on the worker side).
    /// This is intended for deterministic testing.
    pub fn chunks(&mut self) -> impl Iterator<Item = PcmChunk>;

    /// Request a best-effort seek by time.
    pub fn seek(&mut self, position: std::time::Duration) -> Result<(), AudioError>;

    /// Returns the last known output spec (after decoder init).
    pub fn output_spec(&self) -> Option<AudioSpec>;
}
```

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

With feature `rodio`, the crate provides an adapter that exposes the pipeline as a `rodio::Source<Item = f32>`.

Key constraints:
- rodio pulls samples on an audio thread; the adapter must **not block** that thread.
- therefore the adapter should:
  - poll `try_next_chunk()`
  - use an internal small stash/buffer for partial consumption of a chunk
  - output silence (or handle underrun explicitly) if desired by policy

```/dev/null/stream_download_audio_spec.md#L242-260
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