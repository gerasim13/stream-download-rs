# Execution-Focused Refactor Task List (Kanban-ready)

Этот файл — источник kanban-tickets для рефакторинга. Каждая задача содержит:
- **NOTE (core invariant)**: мы не трогаем `crates/stream-download` (core)
- **Problem**: что именно сейчас сломано/плохо
- **Goal**: что хотим получить
- **Actions**: конкретные шаги (включая что почитать и что изменить)
- **Outputs**: какие артефакты появятся (файлы/типы/сигнатуры)
- **DoD**: критерии завершения
- **Depends on**: зависимости (если есть)

> Baseline: `crates/stream-download` уже откатан и является “чистым” core. Мы **не меняем** его публичные контракты.
>
> ВАЖНО (ограничение проекта): **никакого `std::fs` и “прямого доступа к диску” в бизнес-логике/кэше/HLS/тестах/примерах.**
> Любая запись/чтение может происходить **только через `StorageReader/StorageWriter`**, которые предоставляет `StorageProvider`.
> Если нужна “persistent across runs” семантика, она достигается только через специальный `StorageProvider`,
> который внутри себя использует ОС/ФС, но наружу предоставляет лишь `StorageReader/StorageWriter`.
>
> ВАЖНО (data model): **везде используем `bytes::Bytes`**, а не `Vec<u8>`.
>
> ВАЖНО (no legacy): это новая архитектура. Мы **не сохраняем обратную совместимость** со старым message-based API
> (`StreamMsg/StreamControl/StorageHandle/...`). Если что-то ломается — правим вызовы/тесты/примеры под новую модель.

---

## Глобальные правила (все задачи)

1) **Не изменять** публичные контракты `crates/stream-download` (core).
2) HLS должен работать поверх core:
   - `SourceStream: TryStream<Ok = Bytes>`
   - `StorageProvider::into_reader_writer(self, Option<u64>)`
   - HTTP в core байтовый (`Stream<Item = Result<Bytes, _>>`)
3) `std::fs` (и аналоги) запрещены в:
   - `stream-download-cache`
   - `stream-download-hls` бизнес-логике
   - `tests/` и `examples/`
   Разрешены только внутри реализации `StorageProvider`.
4) Кэш-слой должен уметь **оборачивать любой `StorageProvider`** и вести себя как `StorageProvider`, при этом отдавая handler.
5) Ключевое ограничение core: **мы не можем “взять” `StorageReader/StorageWriter` напрямую**.
   Единственный способ получить их — вызвать `StorageProvider::into_reader_writer(self, content_length)` (consuming).
   Любая обёртка/кэш обязаны это учитывать.
6) KV-кэш для HLS реализуется не “универсальным KV поверх любого provider”, а через **специализированный multi-object cache StorageProvider backend**,
   который предоставляет “factory semantics” *внутри себя*, оставаясь `StorageProvider` наружу.

---

# Epic 0 — Baseline & Cutover Strategy

## TASK 0.1 — Define sanity commands (optional but recommended)
**NOTE (core invariant):** В рамках этой задачи (и всех остальных) **запрещено изменять код `crates/stream-download` (core)**.

**Problem:** Во время рефакторинга сборка будет периодически “красной”. Без минимального набора команд легко потерять контроль над прогрессом.

**Goal:** Зафиксировать минимальный набор команд, которые ты запускаешь после каждого ticket’а, чтобы понимать, что именно зелёное/красное.

**Actions:**
1) Убедиться, что baseline-коммит существует и `cargo check -p stream-download` зелёный.
2) Зафиксировать внизу файла команды для:
   - проверки core,
   - проверки cache-крейта,
   - проверки HLS,
   - проверки тестов и примеров.

**Outputs:**
- Обновлённый блок “Sanity commands (manual)” в конце файла.

**DoD:**
- Ты реально используешь этот блок между ticket’ами (он отражает актуальные команды проекта).

**Depends on:** baseline commit.

---

# Epic 1 — New crate: stream-download-cache (single-object BlobCache + StorageProvider wrapper)

## TASK 1.1 — Add new crate `crates/stream-download-cache` (BlobCache + implementation; no KV, no fs)
**NOTE (core invariant):** В рамках этой задачи **нельзя менять `crates/stream-download` (core)**.

**Problem:** В baseline core отсутствуют `StorageHandle/StorageResourceReader` и любые “handles”. При этом нужен общий кэш-слой, который не использует `std::fs` и работает только через `StorageProvider` → `StorageReader/StorageWriter`.

**Goal:** Создать новый крейт `stream-download-cache`, который предоставляет:
1) `BlobCache` (без ключа): `get/put/len/exists` для кэширования **одного объекта** (например, mp3),
2) реализацию `BlobCache` поверх **одного** `StorageProvider` (через `StorageReader/StorageWriter`), без `std::fs`,
3) “handler” в виде `Arc<dyn BlobCache>`.

**Key design constraint:**
Мы не делаем “универсальный KV поверх любого StorageProvider”, потому что у `StorageReader/Writer` нет доступа к именам файлов/директорий и нет namespace.
Стандартный core `StorageProvider` хранит один объект, поэтому универсальный кэш поверх него тоже **одиночный**.

**Actions:**
1) Прочитать baseline core storage контракт:
   - `crates/stream-download/src/storage/mod.rs`
2) Создать `crates/stream-download-cache/Cargo.toml` и `crates/stream-download-cache/src/lib.rs`.
3) Определить trait `BlobCache` (везде `Bytes`, не `Vec<u8>`):
   - `get(&self) -> io::Result<Option<Bytes>>`
   - `put(&self, data: Bytes) -> io::Result<()>`
   - `len(&self) -> io::Result<Option<u64>>`
   - `exists(&self) -> io::Result<bool>`
4) Реализовать `StorageBackedBlobCache<P>` (название на твой выбор), который:
   - принимает `P: StorageProvider`,
   - в `new(provider: P) -> io::Result<Self>` вызывает `provider.into_reader_writer(None)` и сохраняет reader/writer внутри себя,
   - хранит данные как “один объект”:
     - `put`: overwrite/truncate (seek to 0, write_all, flush; при необходимости set_len)
     - `get`: read_to_end в `BytesMut` → freeze в `Bytes`
     - `len/exists`: длина по reader/writer (seek End(0)); 0 трактуем как MISS (или как HIT пустого файла — зафиксировать семантику явно)
5) Подключить крейт в workspace root `Cargo.toml`.
6) Тесты ДОЛЖНЫ быть интеграционными в `crates/stream-download-cache/tests/` (а не только unit tests в `src/`):
   - это заставляет crate иметь отдельный test target и проверяет публичный API “как его будет использовать мир”.
7) Интеграционные тесты должны сразу же прогоняться на core провайдерах хранения (через dev-dependency на `stream-download`):
   - `MemoryStorageProvider` (обязательно)
   - `TempStorageProvider` (если доступен через feature в core; если нет — пропустить и оставить TODO)
8) Набор проверок (везде `Bytes`):
   - put→get roundtrip (Bytes)
   - exists/len для miss/hit
   - overwrite put (два put подряд возвращают второе значение)

**Outputs:**
- `crates/stream-download-cache/Cargo.toml` (включая dev-dependency на `stream-download` для тестов)
- `crates/stream-download-cache/src/lib.rs` (типы: `BlobCache`, `StorageBackedBlobCache`)
- `crates/stream-download-cache/tests/*` (интеграционные тесты, которые используют core storage providers)
- rustdoc в `lib.rs`: “без std::fs, всё через StorageProvider”

**DoD:**
- `cargo check -p stream-download-cache` проходит.
- Интеграционные тесты в `crates/stream-download-cache/tests/` проходят на core storage providers (минимум `MemoryStorageProvider`).
- В реализации нет прямых `std::fs`/path-based операций.
- Публичные API возвращают/принимают `Bytes`.

**Depends on:** baseline core commit.

---

## TASK 1.4 — Add `CachedStorageProvider<StreamP, BlobCacheP>` wrapper that implements core `StorageProvider` and vends a BlobCache handle
**NOTE (core invariant):** В рамках этой задачи **нельзя менять `crates/stream-download` (core)**.

**Problem:** Мы не можем получить `StorageReader/StorageWriter` напрямую; только через consuming `into_reader_writer`. Нам нужен “зонтик”:
- вести себя как обычный `StorageProvider` для `StreamDownload`,
- но отдавать BlobCache handle для кэширования “одного объекта” (универсально, не HLS-only).

**Goal:** В `crates/stream-download-cache` появляется “матрёшка”:
- `CachedStorageProvider<StreamP, BlobCacheP>` реализует core `StorageProvider` и делегирует stream I/O в `stream: StreamP`,
- одновременно создаёт `StorageBackedBlobCache` поверх **отдельного** `blob_cache_provider: BlobCacheP` (через `into_reader_writer(None)` внутри `new(...)`),
- отдаёт `Arc<dyn BlobCache>` через `blob_cache()`.

**Actions:**
1) Прочитать baseline core storage контракт:
   - `crates/stream-download/src/storage/mod.rs`
2) В `crates/stream-download-cache` добавить:
   - `pub struct CachedStorageProvider<StreamP, BlobCacheP> { stream: StreamP, blob_cache: Arc<dyn BlobCache> }`
3) Реализовать `new(stream: StreamP, blob_cache_provider: BlobCacheP) -> io::Result<Self>`:
   - создать `StorageBackedBlobCache::new(blob_cache_provider)` (внутри вызов `into_reader_writer(None)`)
4) Реализовать `StorageProvider` для wrapper:
   - `type Reader = StreamP::Reader`
   - `type Writer = StreamP::Writer`
   - `into_reader_writer(self, content_length: Option<u64>)` → `self.stream.into_reader_writer(content_length)`
   - `max_capacity(&self)` → `self.stream.max_capacity()`
5) Добавить `pub fn blob_cache(&self) -> Arc<dyn BlobCache>` (clone Arc)
6) Unit-тест:
   - stream provider: `MemoryStorageProvider`
   - blob_cache_provider: `MemoryStorageProvider`
   - проверить делегирование `into_reader_writer`
   - проверить `blob_cache().put/get` (Bytes)

**Outputs:**
- `CachedStorageProvider<StreamP, BlobCacheP>` + `blob_cache()` handle
- Нет ожиданий “writer/reader уже есть”: всё создаётся через `into_reader_writer` внутри wrapper/cache.

**DoD:**
- Wrapper компилируется и реализует core `StorageProvider`.
- `blob_cache()` возвращает рабочий `BlobCache` (Bytes).
- Unit-тест подтверждает делегирование и работу blob cache.

**Depends on:** TASK 1.1

---

# Epic 4 — HLS KV cache + lease/eviction (HLS-only, multi-object backend provider)

## TASK 4.1 — Replace missing core file storage with an HLS-internal `StorageProvider` implementation (FS only inside provider)
**NOTE (core invariant):** В рамках этой задачи **не изменяем `crates/stream-download` (core)**.

**Problem:** `stream-download-hls` импортит `stream_download::storage::file::FileStorageProvider`, которого нет в baseline core. При этом OS/FS доступ разрешён только внутри `StorageProvider`.

**Goal:** В `stream-download-hls` появляется *внутренний* `StorageProvider`, который:
- реализует core `StorageProvider` и выдаёт `StorageReader/StorageWriter`,
- может быть disk-backed, но использует OS/FS строго **внутри provider**,
- снимает блокер компиляции вокруг “file storage” зависимостей HLS.

**Actions:**
1) Прочитать baseline core storage контракт:
   - `crates/stream-download/src/storage/mod.rs`
2) Прочитать текущую HLS storage/factory модули, которые импортят отсутствующий file provider:
   - `crates/stream-download-hls/src/storage/hls_factory.rs`
3) Реализовать HLS-internal provider (например `DiskFileStorageProvider`) в `crates/stream-download-hls/src/storage/*`:
   - внутри себя открывает/создаёт файлы/директории, но наружу отдаёт только reader/writer
4) Обновить imports в HLS, чтобы не ссылаться на core file provider.

**Outputs:**
- Новый HLS-internal disk-backed `StorageProvider` (FS only inside)
- HLS код больше не импортит `stream_download::storage::file::*`

**DoD:**
- HLS компилируется без `stream_download::storage::file`.
- FS операции присутствуют только внутри provider.

**Depends on:** baseline core commit.

---

## TASK 4.2 — Introduce HLS KV cache handle backed by a dedicated multi-object cache StorageProvider backend (factory semantics inside provider)
**NOTE (core invariant):** В рамках этой задачи **не меняем `crates/stream-download` (core)**.

**Problem:** HLS нужно кэшировать множество объектов по ключу (playlists/keys/segments/meta). Это KV-операции. Мы не можем:
- использовать `std::fs` напрямую в логике,
- сделать “универсальный KV поверх любого provider”,
- получить имена файлов/директорий через `StorageReader/Writer`.

**Goal:** В `stream-download-hls` появляется HLS-only KV cache handle (Arc), который предоставляет:
- `get(key) -> io::Result<Option<Bytes>>`
- `put(key, Bytes) -> io::Result<()>`
- `len(key) -> io::Result<Option<u64>>`
- `exists(key) -> io::Result<bool>`

и реализуется через **dedicated multi-object cache StorageProvider backend**, который:
- живёт внутри `stream-download-hls`,
- имеет namespace “key → object” *внутри себя* (factory semantics),
- выполняет любые FS операции только внутри provider,
- наружу предоставляет только операции через `StorageReader/StorageWriter` для конкретного key-объекта.

**Key design:**
Мы вводим HLS-only backend interface, который не трогает core, например:
- `trait HlsCacheBackend: Send + Sync { fn open(&self, key: &HlsCacheKey) -> io::Result<(Reader, Writer)>; fn delete(&self, key: &HlsCacheKey) -> io::Result<()>; fn list(&self) -> io::Result<Vec<HlsCacheKey>>; }`
и конкретная реализация backend’а использует один или несколько `StorageProvider` внутри себя (disk-backed или memory-backed), но вся FS-логика остаётся внутри provider.

**Actions:**
1) Прочитать текущие места использования cache keys:
   - `crates/stream-download-hls/src/cache/keys.rs`
   - `crates/stream-download-hls/src/downloader/cache.rs`
   - `crates/stream-download-hls/src/worker.rs` (segment probing)
2) Ввести HLS-only тип ключа:
   - `struct HlsCacheKey(Arc<str>)` (или reuse существующего, но без core зависимостей)
3) Ввести HLS-only KV cache trait (handle), например `HlsKvCache`:
   - `get/put/len/exists` на `Bytes`
4) Реализовать KV cache поверх dedicated backend:
   - backend должен уметь “open per key” и “delete per key” (без прямого std::fs в логике)
   - `put`: overwrite/append в writer для key-объекта
   - `get`: read_to_end из reader → `Bytes`
   - `len`: seek End(0)
5) Протащить `Arc<dyn HlsKvCache>` в builder/worker wiring.

**Outputs:**
- HLS-only KV cache trait + реализация
- Dedicated backend abstraction с factory semantics (open/delete/list) *внутри HLS*

**DoD:**
- KV cache работает для playlists/keys/segments (Bytes).
- Нет прямого `std::fs` в HLS логике: только внутри backend/provider.
- Есть `delete`-возможность для eviction (нужно для TASK 4.3).

**Depends on:** TASK 4.1 (если нужен disk-backed backend), baseline core commit.

---

## TASK 4.3 — Add lease + eviction policy to HLS KV cache (bounded growth, concurrency-safe)
**NOTE (core invariant):** В рамках этой задачи **не меняем `crates/stream-download` (core)**.

**Problem:** Нужна политика “кэш не растёт бесконтрольно”: TTL/LRU/лимит объектов. Ранее это делал `HlsCacheLayer` через `.lease` файлы, но прямой fs запрещён в логике.

**Goal:** KV cache получает политику:
- TTL (lease freshness),
- LRU (access ordering),
- лимит по количеству объектов (или общий бюджет),
- best-effort eviction при превышении лимита,
- корректная работа в многопоточном окружении.

**Actions:**
1) Прочитать текущий lease/eviction код как источник требований:
   - `crates/stream-download-hls/src/storage/cache_layer.rs` (HlsCacheLayer)
2) Встроить lease index (без fs):
   - in-memory индекс: `key/master_hash -> last_access(SystemTime)` + счётчики
   - persist индекса (опционально) через отдельный blob внутри backend (тоже через provider)
3) Hooks:
   - on get/put → `touch(key)`
4) Eviction:
   - выбрать кандидатов (LRU + неактивные по TTL)
   - вызвать `backend.delete(key)` и удалить из in-memory state
5) Concurrency:
   - state под `Mutex/RwLock`
   - eviction best-effort, не блокирует hot path надолго (выполнять редко/по порогам)

**Outputs:**
- Lease/LRU/eviction слой внутри HLS KV cache
- Настройки (лимит/ttl) подключены к `HlsSettings` (если нужно)

**DoD:**
- KV cache ограничивает рост и делает eviction best-effort.
- Нет прямого std::fs в логике.
- Многопоточность корректна (нет data races, нет долгих блокировок на hot path).

**Depends on:** TASK 4.2

---

# Epic 1 (HLS adoption of KV handle)

## TASK 1.2 — Replace HLS small-resource caching (playlists/keys) to use HLS KV cache handle
**NOTE (core invariant):** В рамках этой задачи **не трогаем `crates/stream-download` (core)**.

**Problem:** CacheDownloader кэширует playlists/keys — это множество объектов по ключу. BlobCache из `stream-download-cache` не подходит.

**Goal:** CacheDownloader работает с HLS KV cache handle:
- `get(key)` before fetch,
- `put(key, bytes)` after fetch,
без StoreResource/StorageHandle и без std::fs в логике.

**Actions:**
1) Прочитать текущий `CacheDownloader` и builder:
   - `crates/stream-download-hls/src/downloader/cache.rs`
   - `crates/stream-download-hls/src/downloader/builder.rs`
2) Убрать зависимости от `StorageHandle` и `StoreResource` сообщений.
3) Подключить `Arc<dyn HlsKvCache>` (из TASK 4.2).
4) Обновить builder wiring так, чтобы CacheDownloader получал KV handle при построении chain.

**Outputs:**
- CacheDownloader больше не использует StoreResource/StorageHandle
- CacheDownloader зависит от HLS KV cache handle

**DoD:**
- `cache.rs`/`builder.rs` не импортят `StorageHandle` и не создают `StoreResource` messages
- CacheDownloader делает `get/put` через KV handle

**Depends on:** TASK 4.2

---

## TASK 1.3 — Replace segment-cache probing to use HLS KV cache handle
**NOTE (core invariant):** В рамках этой задачи **не изменяем `crates/stream-download` (core)**.

**Problem:** Worker делает segment probing по ключу (`<master>/<variant>/<basename>`). Это KV.

**Goal:** Worker использует HLS KV cache handle для `len/exists`, чтобы принимать HIT/MISS без std::fs и без StorageHandle.

**Actions:**
1) Прочитать probing места:
   - `crates/stream-download-hls/src/worker.rs` (`cached_segment_key`, `probe_cached_segment_len`)
2) Убрать `StorageHandle` и заменить на KV handle:
   - `len(key)` → HIT/MISS
3) Treat errors as miss (best-effort)

**Outputs:**
- Worker не импортит `StorageHandle`
- Segment probing использует KV cache handle

**DoD:**
- В worker нет импорта `StorageHandle`
- HIT/MISS логика работает через KV handle

**Depends on:** TASK 4.2

---

# Epic 2 — HLS HTTP downloader: bytes-only + DRM headers support

## TASK 2.1 — Rewrite `HttpDownloader` (HLS) to be bytes-based and support key headers via reqwest
**NOTE (core invariant):** В рамках этой задачи **не меняем `crates/stream-download` (core)**.

**Problem:** Текущий `HttpDownloader` в HLS завязан на `StreamMsg` и несуществующий `create_with_headers`. После baseline core HTTP — байтовый и не умеет arbitrary headers в своём `Client` трейте, но DRM тесты требуют key request headers.

**Goal:** `HttpDownloader` становится корректным байтовым downloader’ом:
- `download_with_headers(...)` умеет arbitrary headers (для key DRM),
- `stream(...)` и `stream_range(...)` возвращают `ByteStream = Stream<Item = Result<Bytes, HlsError>>`,
- `probe_content_length(...)` работает через range запрос и не зависит от `StreamMsg`.

**Actions:**
1) Прочитать текущую реализацию:
   - `crates/stream-download-hls/src/downloader/base.rs`
   - `crates/stream-download-hls/src/crypto/resolver.rs`
2) Прочитать baseline core HTTP контракт:
   - `crates/stream-download/src/http/mod.rs`
   - `crates/stream-download/src/http/reqwest_client.rs`
3) Переписать `downloader/base.rs`:
   - убрать `StreamMsg`
   - `download_with_headers` через reqwest (headers; key_request_headers только для `Resource::Key`)
   - `stream/stream_range` через reqwest bytes_stream
   - `probe_content_length` через GET Range 0-0 + Content-Range
4) Маппинг ошибок → `HlsError`

**Outputs:**
- `downloader/base.rs` bytes-only, headers supported for keys

**DoD:**
- `downloader/base.rs` не импортит `StreamMsg`
- headers для keys поддерживаются (логически и через тесты позже)
- методы работают с `Bytes`

**Depends on:** baseline core commit

---

# Epic 3 — HLS becomes a bytes `SourceStream` (core-compatible)

## TASK 3.1 — Make `HlsStream` implement core `SourceStream` (Bytes) and update `HlsStreamParams`
**NOTE (core invariant):** В рамках этой задачи **строго запрещено менять `crates/stream-download` (core)**.

**Problem:** Сейчас `HlsStream` отдаёт message-based `StreamMsg`, а core требует `TryStream<Ok=Bytes>`. Также `HlsStreamParams` завязан на `StorageHandle`, которого нет в baseline core.

**Goal:** `HlsStream` становится байтовым `SourceStream`:
- `Stream<Item = Result<Bytes, HlsError>>`
- `content_length() -> None`
- `HlsStreamParams` принимает KV cache handle (`Arc<dyn HlsKvCache>`) и/или другие зависимости HLS без StorageHandle
- seek/reconnect остаются командными через `HlsCommand`

**Actions:**
1) Прочитать core `SourceStream` контракт:
   - `crates/stream-download/src/source/mod.rs` (верх файла)
2) Прочитать текущий `HlsStream`:
   - `crates/stream-download-hls/src/stream.rs`
3) Переписать `HlsStream` на bytes stream и core SourceStream сигнатуры
4) Обновить `HlsStreamParams`:
   - убрать `StorageHandle`
   - добавить нужные HLS handles (KV cache handle, settings, etc.)

**Outputs:**
- `stream.rs` больше не использует `StreamMsg/StorageHandle`
- `HlsStreamParams` обновлён

**DoD:**
- `HlsStream` удовлетворяет core `SourceStream`
- `content_length() -> None`
- `Bytes` everywhere

**Depends on:** TASK 4.2 (KV handle), TASK 2.1 (bytes downloader) желательно

---

## TASK 3.2 — Make `HlsStreamWorker` send only bytes to the stream channel (no control)
**NOTE (core invariant):** В рамках этой задачи **не трогаем `crates/stream-download` (core)**.

**Problem:** Worker сейчас шлёт control в data channel, но core pipeline должен быть bytes-only.

**Goal:** Worker отправляет только `Result<Bytes, HlsError>` в data channel. Boundaries идут через:
- `StreamEvent` (внешние события),
- direct-boundary API сегментного storage (Epic 5).

**Actions:**
1) Прочитать worker:
   - `crates/stream-download-hls/src/worker.rs`
2) Заменить тип data_sender на bytes
3) Удалить отправку control сообщений в data channel
4) Временно cache-hit можно деградировать до network (до TASK 5.3), но без поломки компиляции

**Outputs:**
- worker bytes-only на data канале

**DoD:**
- нет импорта `StreamMsg/StreamControl`
- data channel bytes-only

**Depends on:** TASK 3.1

---

# Epic 5 — Segmented storage rework (no StorageWriter extensions, direct boundaries)

## TASK 5.1 — Rewrite `SegmentedStorageProvider` to match core `StorageProvider` and remove StoreResource/tree-handle coupling
**NOTE (core invariant):** В рамках этой задачи **нельзя менять `crates/stream-download` (core)**.

**Problem:** `SegmentedStorageProvider` содержит несуществующие для core расширения (ContentLength/StorageHandle/ProvidesStorageHandle/StoreResource path) и смешивает “segment stitcher” и “resource cache”.

**Goal:** Segmented storage отвечает только за сегменты:
- `StorageProvider::into_reader_writer(self, Option<u64>)`,
- state: streams + default stream key,
- нет `StorageHandle`, нет `TreeStorageResourceReader`, нет `store_resource(...)`.

**Actions:**
1) Прочитать baseline core storage контракт:
   - `crates/stream-download/src/storage/mod.rs`
2) Прочитать текущий segmented storage:
   - `crates/stream-download-hls/src/storage/segmented.rs`
3) Привести `into_reader_writer` к `Option<u64>` и удалить всё, что связано с StoreResource/handles
4) Сохранить инварианты `writer.stream_position()`.

**Outputs:**
- `segmented.rs` соответствует core `StorageProvider` и не содержит resource-cache логики

**DoD:**
- нет импорта StorageHandle/ProvidesStorageHandle/StorageResourceReader
- нет store_resource пути

**Depends on:** TASK 4.1

---

## TASK 5.2 — Replace control-based API with direct writer boundary methods (HLS-only)
**NOTE (core invariant):** В рамках этой задачи **не трогаем `crates/stream-download` (core)**.

**Problem:** Старый дизайн ожидал `StorageWriter::control(StreamControl)`; core такого не поддерживает.

**Goal:** В `SegmentedWriter` появляются HLS-only методы:
- `set_default_stream_key(...)`,
- `begin_segment(...)`,
- `end_segment(...)`,
и worker вызывает их напрямую.

**Actions:**
1) Удалить `impl StorageWriter for SegmentedWriter` с control/get_available_ranges_for
2) Ввести HLS-local типы для boundaries
3) В worker вызвать begin/end/set_default_stream_key

**Outputs:**
- boundaries больше не message-based

**DoD:**
- `StreamControl` отсутствует
- корректный `writer.stream_position()`

**Depends on:** TASK 3.2, TASK 5.1

---

## TASK 5.3 — Rebuild “cache hit without network” via virtual read-only segments
**NOTE (core invariant):** В рамках этой задачи **не меняем `crates/stream-download` (core)**.

**Problem:** Раньше cache-hit материализовывался через `ChunkStart/ChunkEnd` без payload. После удаления control нужен новый механизм.

**Goal:** На cache-hit worker добавляет в segmented storage “виртуальный сегмент” (read-only), чтобы избежать сети и двойного IO.

**Actions:**
1) В segmented writer добавить `add_cached_segment(...)`
2) В worker на HIT использовать KV cache `len/exists` и добавить виртуальный сегмент
3) Не стримить payload bytes для cache-hit сегмента

**Outputs:**
- cache-hit работает без сети (best-effort)

**DoD:**
- cache-hit не вызывает network (где возможно)
- seek/read корректны после миграции тестов

**Depends on:** TASK 5.2, TASK 4.2, TASK 5.1

---

# Epic 6 — Tests, fixtures, examples migration (no std::fs)

## TASK 6.1 — Update fixtures storage boxing to core `Option<u64>` and remove control APIs
**NOTE (core invariant):** В рамках этой задачи **не меняем `crates/stream-download` (core)**.

**Problem:** fixtures используют старые control APIs и расширенные ContentLength.

**Goal:** fixtures совместимы с baseline core storage traits.

**Actions:**
1) Правки `tests/tests/fixtures/hls.rs`:
   - убрать control/get_available_ranges_for
   - ContentLength → Option<u64>

**Outputs:**
- fixtures компилируются

**DoD:**
- нет StreamControl/StorageHandle/расширенных типов

**Depends on:** baseline core commit

---

## TASK 6.2 — Replace fixtures StorageHandle with HLS KV cache handle + (optional) BlobCache wrapper
**NOTE (core invariant):** В рамках этой задачи **не меняем `crates/stream-download` (core)**.

**Problem:** fixtures строят provider+handle через fs. Это запрещено.

**Goal:** fixtures собирают:
- StreamDownload storage provider (stream storage)
- HLS KV cache handle (Arc) из TASK 4.2
- (опционально) BlobCache wrapper из stream-download-cache для non-HLS сценариев

**Actions:**
1) Переписать `build_storage` в `tests/tests/fixtures/hls.rs` без std::fs
2) Убрать clean_dir/dir_nonempty_recursive/count_files_recursive проверки (или переписать на request-count/caching behaviour)
3) Прокинуть KV handle в `HlsStreamParams`

**Outputs:**
- fixtures без std::fs

**DoD:**
- tests/fixtures не используют std::fs
- HLS tests проверяют cache через повторный запуск/счётчики запросов

**Depends on:** TASK 4.2, TASK 3.1

---

## TASK 6.3 — Rewrite tests that waited for ChunkStart / ordered controls
**NOTE (core invariant):** В рамках этой задачи **не меняем `crates/stream-download` (core)**.

**Problem:** тесты ждут StreamMsg::Control.

**Goal:** тесты используют warmup read или StreamEvent.

**Actions:**
- переписать helpers в `tests/tests/hls_tests.rs` и fixtures

**Outputs:**
- tests без ожиданий control

**DoD:**
- нет ожиданий StreamMsg::Control

**Depends on:** TASK 3.1–3.2

---

## TASK 6.4 — Update DRM tests to new caching model (no StoreResource)
**NOTE (core invariant):** В рамках этой задачи **не меняем `crates/stream-download` (core)**.

**Problem:** DRM tests завязаны на StoreResource.

**Goal:** DRM tests проверяют headers/query params и KV caching.

**Actions:**
- обновить `tests/tests/hls_tests.rs`

**Outputs:**
- DRM tests обновлены

**DoD:**
- DRM tests проходят (после полной миграции)

**Depends on:** TASK 2.1, TASK 4.2, TASK 3.1

---

## TASK 6.5 — Update examples to new API (no ProvidesStorageHandle)
**NOTE (core invariant):** В рамках этой задачи **не меняем `crates/stream-download` (core)**.

**Problem:** примеры завязаны на StorageHandle.

**Goal:** примеры используют новые handles (BlobCache/KV cache) и новые params.

**Actions:**
- обновить `examples/examples/*`

**Outputs:**
- examples обновлены

**DoD:**
- `cargo check --examples` проходит

**Depends on:** TASK 3.1, TASK 4.2, TASK 1.1

---

# Recommended execution order (Kanban)

1) TASK 1.1  
2) TASK 1.4  
3) TASK 4.1  
4) TASK 4.2  
5) TASK 4.3  
6) TASK 1.2  
7) TASK 1.3  
8) TASK 2.1  
9) TASK 3.1  
10) TASK 3.2  
11) TASK 5.1  
12) TASK 5.2  
13) TASK 5.3  
14) TASK 6.1  
15) TASK 6.2  
16) TASK 6.3  
17) TASK 6.4  
18) TASK 6.5  
19) TASK 0.1 (можно в любой момент)

---

# Sanity commands (manual)

- Core: `cargo check -p stream-download`
- Cache crate: `cargo test -p stream-download-cache`
- HLS: `cargo check -p stream-download-hls`
- All tests: `cargo test`
- Examples: `cargo check --examples`
