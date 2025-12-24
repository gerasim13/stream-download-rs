# stream-download-hls

HLS (HTTP Live Streaming) support for the `stream-download` ecosystem.

This crate implements:

- playlist parsing (master + media),
- variant/segment iteration (`HlsManager`),
- integration into the `stream-download` streaming model (`HlsStream` + `HlsStreamWorker`),
- segmented persistence helpers (store init/media segments as separate files while exposing a stitched `Read + Seek` view).

Most of the high-level design/behavior documentation lives in this README to keep rustdoc in `src/` short and aligned with the current implementation.

---

## Contents

- [Key concepts](#key-concepts)
- [Public API map](#public-api-map)
- [High-level architecture](#high-level-architecture)
- [Playlist parsing](#playlist-parsing)
- [Variant selection & ABR](#variant-selection--abr)
- [Segment iteration semantics](#segment-iteration-semantics)
- [URL resolution](#url-resolution)
- [Caching & persistence model](#caching--persistence-model)
- [Segmented storage](#segmented-storage)
- [Seeking & content length](#seeking--content-length)
- [Encryption / decryption (`aes-decrypt` feature)](#encryption--decryption-aes-decrypt-feature)
- [Operational notes & constraints](#operational-notes--constraints)
- [Documentation policy](#documentation-policy)

---

## Key concepts

- **Master playlist**: describes available variants/renditions (bandwidth, codecs, URI to media playlist).
- **Media playlist**: ordered list of segments for a chosen variant; may include an init segment (fMP4).
- **Variant**: a rendition within a master playlist (`VariantId` + `VariantStream`).
- **Segment descriptor**: metadata describing “what to stream next” (URI, duration, sequence, variant id, encryption metadata).
- **Segment data**: actual bytes plus metadata returned by manager-level APIs (init/media segment type + metadata).
- **Small resources**: playlists and encryption keys (cached as full blobs).
- **Media segments**: large payload; persisted via segmented storage rather than “small-resource cache”.

---

## Public API map

This is a quick orientation map of the crate’s major entry points (names are the crate exports):

### Orchestration / Iteration

- `HlsManager`: orchestrates playlists, variant selection, and segment iteration.
- `MediaStream`: player-facing trait implemented by `HlsManager`.
- `SegmentType`, `SegmentData`, `NextSegmentResult`: segment iteration types.

### Stream integration

- `HlsStream`, `HlsStreamParams`: `stream_download::source::SourceStream` implementation and its params.
- `HlsStreamWorker`: background worker that drives streaming, boundaries, seeking, and metrics.
- `StreamEvent`: out-of-band events (variant changes, init/media boundaries).

### Parsing

- `parse_master_playlist`, `parse_media_playlist`
- Types: `MasterPlaylist`, `MediaPlaylist`, `VariantStream`, `MediaSegment`, `InitSegment`,
  `EncryptionMethod`, `KeyInfo`, `SegmentKey`, `CodecInfo`, `ContainerFormat`, `VariantId`

### Downloaders and caching

- `ResourceDownloader`: HTTP downloader (full downloads + stream segments; retries; cancellation).
- `CachedResourceDownloader`, `CachedBytes`, `CacheSource`: read-before-fetch caching for small resources.

### Storage

- `SegmentedStorageProvider`, `SegmentStorageFactory`
- `HlsFileTreeSegmentFactory`: deterministic file-tree segment layout
- `HlsCacheLayer`: `.lease` + optional eviction policy wrapper
- `HlsPersistentStorageProvider`: recommended alias for disk-backed caching

### Encryption (feature-gated)

- Feature `aes-decrypt`
- `Aes128CbcMiddleware`, `KeyProcessorCallback`

---

## High-level architecture

There are two main layers, plus storage helpers:

### 1) `HlsManager` — playlist and segment orchestration

Responsibilities:

- Load and parse master playlist (via `init()`/`load_master()`).
- Expose available variants (`variants()`).
- Select a variant and keep a current media playlist for it.
- Produce init segment (if any) and then media segments, preserving ordering.
- For live playlists, refresh media playlist and control waiting behavior.

**Important:** `HlsManager` can operate independently as a player-friendly segment iterator. It can also be used by the worker layer for descriptor-based streaming.

### 2) `HlsStream` + `HlsStreamWorker` — integrate with `stream-download`

`HlsStream` is a `SourceStream` implementation. It:

- spawns `HlsStreamWorker`,
- exposes an ordered stream of `StreamMsg` (payload bytes + control messages),
- supports seeking via a command channel,
- provides best-effort `content_length()` using a snapshot updated by the worker.

`HlsStreamWorker`:

- iterates segment descriptors (via the ABR/controller path),
- streams segment bytes (network and/or storage),
- emits control boundaries (`StreamControl`) for segmented persistence,
- emits `StreamEvent` out-of-band for variant and boundary notifications,
- handles cancellation, backoff, and seeking.

### 3) Storage helpers — segmented persistence and small-resource reads

This crate provides a segmented storage provider and factories/layers so a single logical stream can be persisted as init/media chunk files while still supporting a stitched `Read + Seek` view.

---

## Playlist parsing

Parsing is implemented as a thin adapter over `hls_m3u8`.

### Master playlist

`parse_master_playlist(data: &[u8]) -> MasterPlaylist` yields a list of `VariantStream`:

- `VariantId` is currently derived from the variant’s index in the parsed list.
- Variant fields are “best effort”; some attributes may be missing.

### Media playlist

`parse_media_playlist(data: &[u8], variant_id: VariantId) -> MediaPlaylist` yields:

- ordered `MediaSegment` list (`sequence` is derived from `media_sequence + index`),
- optional `InitSegment` (fMP4 map),
- `target_duration` (if present),
- `end_list` derived from presence of `#EXT-X-ENDLIST` (treated as the reliable end marker).

Encryption metadata:

- Each `MediaSegment` may carry an effective `SegmentKey`.
- `MediaPlaylist.current_key` is informational (first key found in playlist); the per-segment effective key is on `MediaSegment::key`.

---

## Variant selection & ABR

There are two mechanisms:

### Manual vs AUTO (ABR) selection

At configuration level, selection is controlled by `HlsSettings.variant_stream_selector`:

- returning `Some(VariantId)` => manual selection target (controller starts locked to that variant),
- returning `None` => AUTO selection (ABR-controlled).

### ABR controller behavior (current implementation)

`AbrController` wraps a `MediaStream` and manages:

- estimated throughput (EWMA-based estimator),
- simple buffer estimate (wall-clock based),
- decision to switch variants (`maybe_switch`).

**Important:** the ABR logic is heuristic and relies on:
- advertised `VariantStream.bandwidth` (bps),
- an estimator seeded from initial bandwidth and updated via `on_media_segment_downloaded(...)`.

Switch decision overview:

- If manual mode is active, ABR does not switch away from the target.
- Otherwise, it selects a candidate variant based on estimated throughput and configured safety factors/hysteresis, and throttles switching via `min_switch_interval`.

### Descriptor-based next API for worker

The worker uses a specialized method on `AbrController<HlsManager>`:

- `next_segment_descriptor_nonblocking()`:
  - updates the buffer estimate,
  - builds metrics and runs ABR switching,
  - asks the underlying manager for the next descriptor.

This keeps ABR decisions and descriptor selection in one place, which avoids borrow/ownership issues in the worker.

---

## Segment iteration semantics

There are two “levels” of iteration:

### Manager-level: segment data (`SegmentType`)

`HlsManager` implements `MediaStream`:

- `init()` loads master playlist.
- `select_variant(VariantId)` selects a rendition and loads its media playlist.
- `next_segment()`:
  - returns init segment first if present and not yet sent,
  - then returns media segments in playlist order,
  - for live: waits internally until new segments appear (based on refresh interval logic).
- `next_segment_nonblocking()`:
  - returns a segment if immediately available,
  - returns `NextSegmentResult::NeedsRefresh { wait }` if live playlist has no new segments yet,
  - returns `EndOfStream` when `#EXT-X-ENDLIST` is observed.

### Worker-level: descriptors + streaming

The worker typically uses descriptor iteration (`SegmentDescriptor` / `NextSegmentDescResult`) and streams bytes as an ordered stream of `StreamMsg`. This is the “plumbing layer” needed for segmented storage.

---

## URL resolution

When resolving a relative URI (playlist/segment/key), `HlsManager` uses this base selection:

1. `HlsSettings.base_url` if provided,
2. else the current media playlist URL (if known),
3. else the master playlist URL.

If the input is already an absolute URL, it is preserved.

This behavior allows you to force a specific origin/CDN via `base_url`, while still handling normal HLS relative paths.

---

## Caching & persistence model

This crate itself does not write directly to disk. Persistence is mediated via `stream-download` storage abstractions and control messages.

### Small resources (playlists, keys)

- `CachedResourceDownloader` optionally uses a `StorageHandle` for **read-before-fetch**.
- On cache hit: returns `CachedBytes { source: Cache, bytes }`.
- On miss: downloads and returns `CachedBytes { source: Network, bytes }`.

`HlsManager` uses this pattern and emits `StreamControl::StoreResource { key, data }` only when `source == Network`. Emission is best-effort (if the channel is full/closed, playback continues).

Key derivation for those resources lives in `src/cache/keys.rs`:
- playlists: `"<master_hash>/<playlist_basename>"`
- keys: `"<master_hash>/<variant_id>/<key_basename>"`

`master_hash_from_url` uses the standard library hasher; it is deterministic for a given build/toolchain but not guaranteed stable across Rust versions. If you need cross-version stability, replace this strategy with a fixed hash.

### Media segments (payload)

Segments are not cached via the “small resource” cache wrapper. Segment persistence is handled by the segmented storage provider and factories (see below).

---

## Segmented storage

This crate provides a segmented storage provider that understands ordered chunk boundaries in `StreamMsg` control messages and maps them to per-chunk files.

### Key elements

- `SegmentedStorageProvider<F>`: a `StorageProvider` that:
  - receives control boundaries (`StreamControl`) via its writer,
  - writes each chunk to a dedicated inner provider obtained from a `SegmentStorageFactory`,
  - exposes a stitched `Read + Seek` view via its reader.

- `SegmentStorageFactory`: creates one storage provider per chunk based on:
  - `stream_key` (logical stream identifier),
  - `chunk_id`,
  - `ChunkKind`,
  - `filename_hint` (playlist-derived basename, query ignored).

### HLS-specific file-tree layout

`HlsFileTreeSegmentFactory` maps to:

- `<storage_root>/<master_hash>/<variant_id>/<segment_basename>`

**Conventions required by the storage layer:**
- `stream_key` must be `"<master_hash>/<variant_id>"`.
- `filename_hint` must be derived from the playlist URI basename (query ignored).
- No extension inference is performed; the playlist URI is the source of truth.

### Cache policy layer

`HlsCacheLayer` wraps a segment factory and adds:

- `.lease` marker under `<storage_root>/<master_hash>/`
- TTL freshness checks and LRU ordering based on `.lease` mtime
- optional eviction when `max_cached_streams` is exceeded

This is best-effort policy plumbing; failures should not break playback.

### Recommended provider alias

`HlsPersistentStorageProvider` is the recommended disk-backed caching provider:
- `SegmentedStorageProvider<HlsCacheLayer<HlsFileTreeSegmentFactory>>`

Construct it using:
- `SegmentedStorageProvider::new_hls_file_tree(storage_root, prefetch_bytes, max_cached_streams)`

---

## Seeking & content length

### Seeking

`HlsStream` exposes `SourceStream` seeking by sending byte offsets to the worker.

Under the hood:

- The worker converts a byte offset into a segment + intra-segment offset:
  - via `HlsManager::resolve_position(byte_offset) -> (SegmentDescriptor, intra_offset)`
  - probing segment sizes if needed (best-effort via `probe_content_length`).

- The worker then updates internal counters and may reset manager state accordingly:
  - `HlsManager::seek_to_descriptor(&desc)` adjusts the next segment pointer.

This approach supports forward and backward seeking, but correctness depends on:
- availability of segment sizes (from cache or probe),
- and the currently known media playlist window (live streams may only know a moving window).

### Content length

`HlsStream::content_length()` is best-effort:

- It uses a `SegmentedLength` snapshot stored behind an `RwLock`.
- The worker updates this snapshot as it emits chunk boundaries.
- Until at least one boundary/length is observed, `content_length()` returns `Unknown` to avoid misleading “0” clamping.

---

## Encryption / decryption (`aes-decrypt` feature)

The parser exposes encryption metadata, but actual decryption is optional and feature-gated.

### Feature

Enable:
- `aes-decrypt`

Exports:
- `Aes128CbcMiddleware`
- `KeyProcessorCallback`

### Key fetch customization (settings)

When enabled, the following settings may be used for key fetch requests:

- `HlsSettings.key_query_params`
- `HlsSettings.key_request_headers`
- `HlsSettings.key_processor_cb` to post-process raw key bytes (custom wrapping/unwrapping flows)

### Decryption pipeline

Decryption is applied in the worker pipeline via `StreamMiddleware`:

- For AES-128 segments, the worker resolves key/IV parameters and installs `Aes128CbcMiddleware`.

Important implementation note:

- `Aes128CbcMiddleware` currently buffers the whole segment and decrypts once at the end to remove PKCS#7 padding.
  It is not a true streaming decrypt implementation.

---

## Operational notes & constraints

- **Best-effort persistence:** `StoreResource` is emitted via a channel; if the channel is full/closed, bytes may not be persisted but playback should continue.
- **Live playlists:** `#EXT-X-ENDLIST` is treated as the end-of-stream marker. Without it, the stream is treated as live/event until it stops producing new segments.
- **Variant switching:** The code assumes variants are time-aligned when preserving segment index across variant switches.
- **Deterministic layout:** Segment filenames come from playlist URIs. Query strings are ignored for basenames.
- **Hash stability:** `master_hash_from_url` uses the std hasher; do not treat it as globally stable across Rust versions.

---

## Documentation policy

- Keep rustdoc in `src/` concise:
  - 1–3 lines for public items unless field-level docs are involved.
  - field-level docs are retained (good IDE hover/tooltips and API clarity).
- Put “wall of text” design notes in this README.
- If you add new behavior:
  - update the relevant section here,
  - keep code comments focused on local invariants and sharp edges.