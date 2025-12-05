# stream-download-audio

Unified audio pipeline for `stream-download`: float PCM source with optional HLS/HTTP backends, async resampling, processing hooks, and an optional `rodio` adapter.

Status: early skeleton (PoC). The public API surface is established; the background pipeline (decoding, resampling, effects) will be filled in next iterations.

## Highlights

- One interface for audio playback:
  - Pull-based `FloatSampleSource` returning interleaved `f32` PCM.
  - High-level `AudioStream` backed by a lock-free ring buffer.
- Backends:
  - HLS via `stream-download-hls` (incl. ABR using `AbrController`).
  - HTTP via `stream-download` (single resource).
- Async resampling to the app’s audio session sample rate using `rubato`.
- Explicit processing hooks (`AudioProcessor`) to insert effects in the chain.
- Events & control via `kanal`.
- Optional `rodio` adapter (feature-gated) to plug directly into `rodio::OutputStream`.

## Why this crate?

You may want a simple, unified way to consume audio as interleaved `f32` samples regardless of whether the source is HLS (adaptive, segmented) or a regular HTTP resource (MP3/AAC/etc.). This crate gives you that, with built-in resampling to your device’s or app’s target sample rate and an escape hatch to add future audio effects.

## Features

- `symphonia` (optional): Enable decoding through the Symphonia stack (all formats/codecs via default features). Intended to power the HTTP and HLS decode stages.
- `rodio` (optional): Enable a `rodio` adapter that implements `rodio::Source<Item=f32>` on top of `AudioStream`.

No default features are enabled.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
stream-download-audio = { version = "0.0.1", path = "../stream-download-audio", features = ["symphonia"] }

# Optional: to use the rodio adapter
stream-download-audio = { version = "0.0.1", path = "../stream-download-audio", features = ["symphonia", "rodio"] }
```

This crate depends on the sibling crates within the same workspace:
- `stream-download`
- `stream-download-hls`

In this repository they are already wired via path dependencies.

## Quickstart

### HLS to rodio (with `rodio` feature)

```rust
use std::sync::{Arc, Mutex};
use stream_download_audio::{
    AudioOptions, AudioStream, HlsConfig, AbrConfig,
    adapt_to_rodio, // requires `rodio` feature
};

// Create an HLS audio stream targeting your device’s sample rate and channels.
let opts = AudioOptions {
    target_sample_rate: 48_000,
    target_channels: 2,
    ..Default::default()
};

let hls_config = HlsConfig::default();
let abr_config = AbrConfig::default();

let stream = AudioStream::from_hls(
    "https://example.com/master.m3u8",
    opts,
    hls_config,
    abr_config,
);

// Wrap into an Arc<Mutex<_>> as the rodio adapter expects interior mutability.
let stream = Arc::new(Mutex::new(stream));

// Create rodio output.
#[cfg(feature = "rodio")]
{
    let (_stream, stream_handle) = rodio::OutputStream::try_default().unwrap();
    let sink = rodio::Sink::try_new(&stream_handle).unwrap();

    // Adapt our AudioStream to a rodio Source and play.
    let source = stream_download_audio::adapt_to_rodio(stream.clone());
    sink.append(source);
    sink.sleep_until_end();
}
```

### HTTP (single resource) to your own engine

```rust
use stream_download_audio::{AudioOptions, AudioStream, FloatSampleSource};

let opts = AudioOptions {
    target_sample_rate: 48_000,
    target_channels: 2,
    ..Default::default()
};

let mut audio = AudioStream::from_http(
    "https://example.com/audio.mp3",
    opts,
);

// Pull interleaved f32 samples (blocking in small bursts).
let mut buf = [0.0f32; 1024];
let n = audio.read_interleaved(&mut buf);
// Feed `buf[..n]` to your audio device...
```

Notes:
- In the current skeleton, decoding/resampling/ABR pipelines are not yet fully wired.
- The examples show the intended usage; the next iterations will connect the background tasks.

## API Overview

- `AudioStream`
  - High-level audio source implementing `FloatSampleSource`.
  - Constructors:
    - `from_hls(url, options, hls_config, abr_config)`
    - `from_http(url, options)`
  - Event subscription: `subscribe_events() -> kanal::Receiver<PlayerEvent>`
  - Optional `rodio` adapter: `adapt_to_rodio(Arc<Mutex<AudioStream>>) -> RodioSourceAdapter` (behind `rodio` feature).

- `FloatSampleSource` (trait)
  - `read_interleaved(&mut [f32]) -> usize`
  - `spec() -> AudioSpec`
  - `seek(Duration)` (optional)
  - `is_eof() -> bool`

- `AudioSpec`
  - `{ sample_rate: u32, channels: u16 }`

- `AudioOptions`
  - `initial_mode: VariantMode` (for HLS)
  - `target_sample_rate: u32`
  - `target_channels: u16`
  - `ring_capacity_frames: usize`
  - `abr_min_switch_interval: Duration`
  - `abr_up_hysteresis_ratio: f32`

- `VariantMode`
  - `Auto | Manual(usize)`

- `PlayerEvent`
  - `Started | VariantSwitched { .. } | FormatChanged { .. } | BufferLevel { .. } | EndOfStream | Error { .. }`

- `AudioProcessor` (trait)
  - `process(&self, pcm: &mut [f32], spec: AudioSpec) -> Result<(), String>`
  - Will be applied in-chain after resampling (future work).

## Backends

- HLS:
  - Uses `HlsManager` and `AbrController` from `stream-download-hls`.
  - Fetches segments, init-segments (fMP4), optionally decrypts (if enabled in HLS crate).
  - ABR decisions driven by time-to-download, buffer metrics, and configured hysteresis.

- HTTP:
  - Uses `stream-download` to fetch a single resource.
  - Decoded by Symphonia (when the `symphonia` feature is enabled).

## Resampling

- Implemented with `rubato` (high-quality, pure-Rust).
- Runs asynchronously; the pipeline resamples decoded PCM to the `AudioOptions.target_sample_rate`.
- The resampler is recreated when the input format changes (e.g., HLS variant switch).

## Processing Pipeline (Effects)

- A pluggable chain of `AudioProcessor` will be executed after resampling and before the ring buffer write.
- This enables future effects (e.g., volume, EQ, limiter) without changing the player API.

## Features

- `symphonia`: enable decoding with Symphonia.
- `rodio`: enable rodio adapter (`rodio::Source<Item=f32>` implementation).

## Roadmap

- Wire decoding (Symphonia) and resampling (rubato) in background tasks.
- Implement HLS variant alignment on live switching (sequence-based).
- Honor HTTP key headers (if required) for DRM endpoints in the HLS crate.
- Add examples:
  - HLS -> rodio
  - HTTP -> rodio
  - Custom `AudioProcessor` (e.g., simple gain).
- Add integration tests (later).

## License

Dual-licensed under MIT or Apache-2.0, at your option. See the workspace licenses for details.
