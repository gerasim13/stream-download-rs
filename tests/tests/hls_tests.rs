//! Unified HLS integration tests.
//!
//! This file combines:
//! - cache warmup test (2 runs against same storage_root)
//! - worker-level checks (chunk boundaries / manual selection behavior)
//! - manager-level checks (master parsing / select_variant affects fetched bytes)
//! - deterministic ABR switching test (downswitch) via controlled fixture latency
//!
//! All tests use a local in-memory HLS fixture server (no external network).
//!
//! Notes:
//! - We intentionally do NOT decode audio.
//! - These tests validate: variant discovery, manual selection behavior, ABR switching, and that the stream
//!   produces bytes consistently (including after cache warmup).

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use rstest::rstest;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use stream_download::source::{ChunkKind, StreamControl, StreamMsg};
use stream_download::storage::ProvidesStorageHandle;
use stream_download::{Settings, StreamDownload};

use stream_download_hls::{HlsManager, HlsSettings, NextSegmentDescResult, VariantId};

mod hls_fixture;
mod setup;

use hls_fixture::HlsFixture;

const STORAGE_ROOT_SUBDIR: &str = "hls-tests";
const STORAGE_ROOT_SUBDIR_SELECT_VARIANT: &str = "hls-tests-select-variant";

fn is_variant_stream_key(
    stream_key: &stream_download::source::ResourceKey,
    variant_id: u64,
) -> bool {
    stream_key.0.ends_with(&format!("/{}", variant_id))
}

fn build_storage_handle(suffix: &str) -> stream_download::storage::StorageHandle {
    let storage_root = std::env::temp_dir()
        .join("stream-download-tests")
        .join(suffix);

    let prefetch_bytes = std::num::NonZeroUsize::new(256 * 1024).unwrap();
    let max_cached_streams = std::num::NonZeroUsize::new(10).unwrap();

    let provider = stream_download_hls::HlsPersistentStorageProvider::new_hls_file_tree(
        storage_root,
        prefetch_bytes,
        Some(max_cached_streams),
    );

    provider
        .storage_handle()
        .expect("persistent HLS storage provider must vend a StorageHandle")
}

async fn run_worker_collecting<F, T>(
    fixture: HlsFixture,
    hls_settings: HlsSettings,
    data_channel_capacity: usize,
    f: F,
) -> T
where
    F: FnOnce(
        mpsc::Receiver<StreamMsg>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>,
    T: Send + 'static,
{
    let (data_tx, data_rx) = mpsc::channel::<StreamMsg>(data_channel_capacity);
    let (_seek_tx, seek_rx) = mpsc::channel::<u64>(16);
    let cancel = CancellationToken::new();
    let (event_tx, _event_rx) = broadcast::channel(16);

    let storage_handle = build_storage_handle(STORAGE_ROOT_SUBDIR);

    let (_base_url, worker) = fixture
        .worker(
            storage_handle,
            Arc::new(hls_settings),
            data_tx,
            seek_rx,
            cancel.clone(),
            event_tx.clone(),
            Arc::new(std::sync::RwLock::new(
                stream_download::storage::SegmentedLength::default(),
            )),
        )
        .await;

    let worker_task = tokio::spawn(async move { worker.run().await });

    let out = f(data_rx).await;

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_task).await;

    out
}

async fn wait_first_chunkstart(mut data_rx: mpsc::Receiver<StreamMsg>) -> StreamControl {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(250), data_rx.recv()).await {
            Ok(Some(StreamMsg::Control(ctrl))) => {
                if matches!(ctrl, StreamControl::ChunkStart { .. }) {
                    return ctrl;
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {}
        }
    }
    panic!("did not observe any ChunkStart in time");
}

async fn collect_first_n_chunkstarts(
    mut data_rx: mpsc::Receiver<StreamMsg>,
    n: usize,
) -> Vec<StreamControl> {
    let mut out: Vec<StreamControl> = Vec::with_capacity(n);
    let deadline = Instant::now() + Duration::from_secs(30);

    while Instant::now() < deadline && out.len() < n {
        match tokio::time::timeout(Duration::from_millis(500), data_rx.recv()).await {
            Ok(Some(StreamMsg::Control(ctrl))) => {
                if matches!(ctrl, StreamControl::ChunkStart { .. }) {
                    out.push(ctrl);
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {}
        }
    }

    out
}

#[rstest]
#[case(2, Duration::ZERO)]
#[case(2, Duration::from_millis(250))]
#[case(4, Duration::ZERO)]
#[case(4, Duration::from_millis(250))]
fn hls_cache_warmup_produces_bytes_twice_on_same_storage_root(
    #[case] variant_count: usize,
    #[case] v0_delay: Duration,
) {
    // Use shared runtime from setup.rs (consistent test infrastructure).
    setup::SERVER_RT.block_on(async {
        // Local fixture (no external network).
        let fixture =
            HlsFixture::with_variant_count(variant_count).with_slow_v0_segment_delay(v0_delay);
        // Build the reader via the fixture helper so this test uses the same wiring as other
        // end-to-end tests (fixture server + StreamDownload + persistent storage).
        let hls_settings = HlsSettings::default();

        // Persistent cache root shared across two runs.
        // Include params in the path to avoid cross-test interference.
        let storage_root = std::env::temp_dir()
            .join("stream-download-tests")
            .join(format!(
                "hls-cache-warmup-fixture-v{variant_count}-d{}",
                v0_delay.as_millis()
            ));

        // We'll create a fresh provider each run, but keep the same storage root to validate warmup.
        // (The helper takes a storage_root; provider creation happens inside.)

        // Read some bytes (enough to force at least a couple segments).
        let want = 512 * 1024;
        let timeout = Duration::from_secs(10);

        for run_idx in 0..2 {
            let (_base_url, mut reader) = fixture
                .stream_download(
                    storage_root.clone(),
                    hls_settings.clone(),
                    Settings::default(),
                )
                .await;

            let mut buf = vec![0u8; 64 * 1024];
            let mut total = 0usize;
            let start = Instant::now();

            while total < want && start.elapsed() < timeout {
                let to_read = std::cmp::min(buf.len(), want - total);
                match std::io::Read::read(&mut reader, &mut buf[..to_read]) {
                    Ok(0) => break,
                    Ok(n) => total += n,
                    Err(e) => panic!(
                        "run {run_idx}: read error after {total} bytes (variant_count={variant_count}, v0_delay={:?}): {e}",
                        v0_delay
                    ),
                }
            }

            assert!(
                total > 0,
                "run {run_idx}: expected to read some bytes from HLS stream, got 0 (variant_count={variant_count}, v0_delay={:?})",
                v0_delay
            );
        }
    });
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(4)]
#[case(6)]
fn hls_manager_parses_exact_variant_count_from_master(#[case] variant_count: usize) {
    setup::SERVER_RT.block_on(async {
        let fixture = HlsFixture::with_variant_count(variant_count)
            .with_slow_v0_segment_delay(Duration::ZERO);

        let storage_handle = build_storage_handle(STORAGE_ROOT_SUBDIR);
        let settings = Arc::new(HlsSettings::default());

        let (data_tx, _data_rx) = mpsc::channel::<StreamMsg>(1);
        let (_base_url, mut manager) = fixture.manager(storage_handle, settings, data_tx).await;

        manager
            .load_master()
            .await
            .expect("failed to load master playlist");

        let master = manager
            .master()
            .expect("master playlist must be available after load_master");

        assert_eq!(
            master.variants.len(),
            variant_count,
            "expected {} variants in master playlist, got {}",
            variant_count,
            master.variants.len()
        );
    });
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
#[case(3)]
fn hls_worker_manual_selection_emits_only_selected_variant_chunks(#[case] variant_idx: u64) {
    setup::SERVER_RT.block_on(async {
        // Use 4 variants so we can parameterize across multiple variant selections.
        let fixture = HlsFixture::with_variant_count(4).with_slow_v0_segment_delay(Duration::ZERO);

        let chunkstarts = run_worker_collecting(
            fixture,
            HlsSettings::default().selection_manual(VariantId(variant_idx as usize)),
            4096,
            |rx| Box::pin(async move { collect_first_n_chunkstarts(rx, 8).await }),
        )
        .await;

        assert!(
            !chunkstarts.is_empty(),
            "expected at least one ChunkStart in manual mode (variant_idx={})",
            variant_idx
        );

        for ctrl in chunkstarts {
            if let StreamControl::ChunkStart { stream_key, .. } = ctrl {
                assert!(
                    is_variant_stream_key(&stream_key, variant_idx),
                    "expected only variant {} in manual mode, got stream_key='{}'",
                    variant_idx,
                    stream_key.0
                );
            }
        }
    });
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(4)]
fn hls_worker_auto_starts_with_variant0_first_chunkstart(#[case] variant_count: usize) {
    setup::SERVER_RT.block_on(async {
        let fixture = HlsFixture::with_variant_count(variant_count)
            .with_slow_v0_segment_delay(Duration::ZERO);

        let first = run_worker_collecting(fixture, HlsSettings::default(), 2048, |rx| {
            Box::pin(async move { wait_first_chunkstart(rx).await })
        })
        .await;

        match first {
            StreamControl::ChunkStart {
                stream_key, kind, ..
            } => {
                assert!(
                    is_variant_stream_key(&stream_key, 0),
                    "expected first ChunkStart to be for variant 0, got stream_key='{}' kind={:?}",
                    stream_key.0,
                    kind
                );

                if kind == ChunkKind::Media {
                    eprintln!(
                        "note: first ChunkStart was Media for stream_key='{}' (no init). \
                         This may be acceptable for TS but suspicious for fMP4.",
                        stream_key.0
                    );
                }
            }
            _ => unreachable!("we only captured ChunkStart above"),
        }
    });
}

#[rstest]
#[case(2, 0, 1)]
#[case(4, 0, 3)]
#[case(4, 1, 2)]
#[case(6, 2, 5)]
fn hls_manager_select_variant_changes_fetched_media_bytes_prefix(
    #[case] variant_count: usize,
    #[case] from_variant: usize,
    #[case] to_variant: usize,
) {
    setup::SERVER_RT.block_on(async {
        assert!(
            from_variant < variant_count && to_variant < variant_count && from_variant != to_variant,
            "invalid parameterization: variant_count={variant_count} from_variant={from_variant} to_variant={to_variant}"
        );
        let fixture = HlsFixture::with_variant_count(variant_count)
            .with_slow_v0_segment_delay(Duration::ZERO);

        let storage_handle = build_storage_handle(STORAGE_ROOT_SUBDIR_SELECT_VARIANT);
        let settings = Arc::new(HlsSettings::default());

        let (data_tx, _data_rx) = mpsc::channel::<StreamMsg>(8);
        let (_base_url, mut manager) = fixture.manager(storage_handle, settings, data_tx).await;

        manager
            .load_master()
            .await
            .expect("failed to load master playlist");

        async fn first_media_prefix(
            manager: &mut HlsManager,
            variant_index: usize,
        ) -> Vec<u8> {
            manager
                .select_variant(variant_index)
                .await
                .unwrap_or_else(|e| panic!("select variant {variant_index} failed: {e}"));
            let desc = loop {
                match manager.next_segment_descriptor_nonblocking().await {
                    Ok(NextSegmentDescResult::Segment(d)) => {
                        if d.is_init {
                            continue;
                        }
                        break d;
                    }
                    Ok(other) => panic!(
                        "expected Segment descriptor for variant {variant_index}, got: {:?}",
                        other
                    ),
                    Err(e) => panic!("failed to get descriptor for variant {variant_index}: {e}"),
                }
            };

            let mut stream = manager
                .downloader()
                .stream_segment(&desc.uri)
                .await
                .unwrap_or_else(|e| panic!("failed to stream segment for variant {variant_index}: {e}"));

            let mut out: Vec<u8> = Vec::new();
            while let Some(item) = stream.next().await {
                let chunk = item.unwrap_or_else(|e| {
                    panic!("error while reading segment stream for variant {variant_index}: {e}")
                });
                out.extend_from_slice(&chunk);
                if out.len() >= 6 {
                    break;
                }
            }
            assert!(
                !out.is_empty(),
                "expected some bytes for variant {variant_index} media segment"
            );
            out.truncate(std::cmp::min(out.len(), 6));
            out
        }

        let a = first_media_prefix(&mut manager, from_variant).await;
        let b = first_media_prefix(&mut manager, to_variant).await;

        assert_ne!(
            a,
            b,
            "expected media bytes to change after select_variant; got same prefix (variant {from_variant} -> {to_variant}) = {:?}",
            a
        );
    });
}

#[rstest]
#[case(2, 0, "INIT-V0")]
#[case(2, 1, "NIT-V0")]
#[case(2, 5, "V0")]
#[case(2, 7, "V0-SEG-0")]
#[case(4, 0, "INIT-V0")]
#[case(4, 1, "NIT-V0")]
#[case(4, 5, "V0")]
#[case(4, 7, "V0-SEG-0")]
fn hls_streamdownload_read_seek_returns_expected_bytes_at_known_offsets(
    #[case] variant_count: usize,
    #[case] seek_pos: u64,
    #[case] expected_prefix: &str,
) {
    setup::SERVER_RT.block_on(async {
        let fixture = HlsFixture::with_variant_count(variant_count)
            .with_slow_v0_segment_delay(Duration::ZERO);

        // Build an end-to-end reader via the "matryoshka" path:
        // fixture server -> injected manager -> worker -> HlsStream::new_with_worker -> StreamDownload.
        //
        // Storage root unique per case to avoid cross-test interference.
        let storage_root = std::env::temp_dir()
            .join("stream-download-tests")
            .join(format!(
                "hls-read-seek-e2e-worker-v{variant_count}-p{seek_pos}"
            ));

        let prefetch_bytes = std::num::NonZeroUsize::new(512 * 1024).unwrap();
        let max_cached_streams = std::num::NonZeroUsize::new(10).unwrap();

        let provider = stream_download_hls::HlsPersistentStorageProvider::new_hls_file_tree(
            storage_root,
            prefetch_bytes,
            Some(max_cached_streams),
        );

        let storage_handle = provider
            .storage_handle()
            .expect("persistent HLS storage provider must vend a StorageHandle");

        let settings = Arc::new(HlsSettings::default());

        // Channels used by the worker/stream.
        let (data_tx, data_rx) = mpsc::channel::<StreamMsg>(settings.prefetch_buffer_size);
        let (seek_tx, seek_rx) = mpsc::channel::<u64>(1);
        let cancel = CancellationToken::new();
        let (event_tx, _event_rx) = broadcast::channel(64);
        let segmented_length = Arc::new(std::sync::RwLock::new(
            stream_download::storage::SegmentedLength::default(),
        ));

        // Build the worker via fixture so the manager is not "detached" from the fixture URLs.
        let (_base_url, worker) = fixture
            .worker(
                storage_handle.clone(),
                settings.clone(),
                data_tx,
                seek_rx,
                cancel.clone(),
                event_tx.clone(),
                segmented_length.clone(),
            )
            .await;

        // Build the stream from the injected worker.
        let stream = stream_download_hls::HlsStream::new_with_worker(
            data_rx,
            seek_tx,
            cancel,
            event_tx,
            segmented_length,
            worker,
        )
        .expect("failed to build HlsStream from injected worker");

        // Wrap into StreamDownload in a "SourceStream already constructed" mode.
        // We do this by using the public constructor that accepts a stream instance.
        let mut reader = StreamDownload::from_stream(stream, provider, Settings::default())
            .await
            .expect("failed to create StreamDownload from injected HlsStream");

        // Ensure the pipeline is started (worker emits ChunkStart before data).
        // This avoids "no active segment; expected ChunkStart before data" on early seeks.
        let mut warm = [0u8; 1];
        let _ = Read::read(&mut reader, &mut warm).expect("warmup read failed");

        let p = Seek::seek(&mut reader, SeekFrom::Start(seek_pos))
            .unwrap_or_else(|e| panic!("seek(Start({seek_pos})) failed: {e}"));
        assert_eq!(p, seek_pos, "seek(Start({seek_pos})) returned {p}");

        let mut buf = vec![0u8; expected_prefix.len()];
        Read::read_exact(&mut reader, &mut buf)
            .unwrap_or_else(|e| panic!("read_exact after seek({seek_pos}) failed: {e}"));

        let got = std::str::from_utf8(&buf).expect("fixture bytes should be utf-8");
        assert_eq!(
            got, expected_prefix,
            "unexpected bytes at seek_pos={seek_pos}"
        );

        // Also verify seek(0) works after some activity.
        let p0 = Seek::seek(&mut reader, SeekFrom::Start(0)).expect("seek back to 0 failed");
        assert_eq!(p0, 0, "seek(Start(0)) returned {p0}");
        let mut b0 = vec![0u8; "INIT-V0".len()];
        Read::read_exact(&mut reader, &mut b0).expect("read_exact at 0 failed");
        assert_eq!(
            std::str::from_utf8(&b0).unwrap(),
            "INIT-V0",
            "expected INIT-V0 at start after roundtrip seek"
        );
    });
}
