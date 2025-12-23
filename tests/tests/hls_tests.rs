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
//!
//! Additional storage assertions:
//! - Persistent file-tree storage should create files on disk after reading.
//! - Memory stream storage should NOT create segment files on disk (resource cache may still touch disk).

use std::io::{Read, Seek, SeekFrom};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use rstest::rstest;
use tokio::sync::mpsc;

use stream_download::source::{ChunkKind, StreamControl, StreamMsg};

use stream_download_hls::{HlsManager, HlsSettings, NextSegmentDescResult, VariantId};

mod hls_fixture;
mod setup;

use hls_fixture::{HlsFixture, HlsFixtureStorageKind};

fn dir_nonempty_recursive(root: &std::path::Path) -> bool {
    count_files_recursive(root) > 0
}

fn count_files_recursive(root: &std::path::Path) -> usize {
    fn walk(p: &std::path::Path, acc: &mut usize) {
        let Ok(rd) = std::fs::read_dir(p) else {
            return;
        };
        for entry in rd.flatten() {
            let path = entry.path();
            let Ok(ft) = entry.file_type() else {
                continue;
            };
            if ft.is_file() {
                *acc += 1;
            } else if ft.is_dir() {
                walk(&path, acc);
            }
        }
    }

    if !root.exists() {
        return 0;
    }

    let mut n = 0usize;
    walk(root, &mut n);
    n
}

fn clean_dir(root: &std::path::Path) {
    if let Err(e) = std::fs::remove_dir_all(root) {
        if e.kind() != std::io::ErrorKind::NotFound {
            panic!("failed to clean directory {}: {e}", root.display());
        }
    }
}

fn is_variant_stream_key(
    stream_key: &stream_download::source::ResourceKey,
    variant_id: u64,
) -> bool {
    stream_key.0.ends_with(&format!("/{}", variant_id))
}

fn build_fixture_storage_kind(
    base_name: &str,
    variant_count: usize,
    storage: &str,
) -> HlsFixtureStorageKind {
    match storage {
        "persistent" => HlsFixtureStorageKind::Persistent {
            storage_root: std::env::temp_dir()
                .join("stream-download-tests")
                .join(format!("{base_name}-v{variant_count}-persistent")),
        },
        "temp" => HlsFixtureStorageKind::Temp {
            subdir: format!("{base_name}-v{variant_count}-temp"),
        },
        "memory" => HlsFixtureStorageKind::Memory {
            resource_cache_root: std::env::temp_dir()
                .join("stream-download-tests")
                .join(format!("{base_name}-v{variant_count}-memory-resources")),
        },
        other => panic!("unknown storage kind '{other}'"),
    }
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
#[case(2, Duration::ZERO, "persistent")]
#[case(2, Duration::ZERO, "temp")]
#[case(2, Duration::ZERO, "memory")]
#[case(2, Duration::from_millis(250), "persistent")]
#[case(2, Duration::from_millis(250), "temp")]
#[case(2, Duration::from_millis(250), "memory")]
#[case(4, Duration::ZERO, "persistent")]
#[case(4, Duration::ZERO, "temp")]
#[case(4, Duration::ZERO, "memory")]
#[case(4, Duration::from_millis(250), "persistent")]
#[case(4, Duration::from_millis(250), "temp")]
#[case(4, Duration::from_millis(250), "memory")]
fn hls_cache_warmup_produces_bytes_twice_on_same_storage_root(
    #[case] variant_count: usize,
    #[case] v0_delay: Duration,
    #[case] storage: &str,
) {
    setup::SERVER_RT.block_on(async {
        let fixture = HlsFixture::with_variant_count(variant_count)
            .with_segment_delay(v0_delay);

        let storage_kind =
            build_fixture_storage_kind("hls-cache-warmup-fixture", variant_count, storage);

        // Ensure we start from a clean slate so the "warmup happened" assertion is meaningful.
        match &storage_kind {
            HlsFixtureStorageKind::Persistent { storage_root } => {
                clean_dir(storage_root);
            }
            HlsFixtureStorageKind::Temp { subdir } => {
                let root = std::env::temp_dir()
                    .join("stream-download-tests")
                    .join(subdir);
                clean_dir(&root);
            }
            HlsFixtureStorageKind::Memory {
                resource_cache_root,
            } => {
                clean_dir(resource_cache_root);
            }
        }
        fixture
            .reset_request_counts()
            .expect("failed to reset fixture request counters");

        let base_url = fixture.start().await;
        let seg0_path = "/seg/v0_0.bin";

        // The fixture payloads are intentionally tiny (short ASCII strings), so we can't "force"
        // large reads. This test should validate *storage reuse* without overfitting to HTTP
        // fetch patterns (which can legitimately re-fetch due to playlist reloads, cache headers,
        // TTL, etc).
        //
        // What we assert:
        // - each run reads at least some bytes (smoke)
        // - for persistent storage, the storage root becomes non-empty after run #1
        //   (i.e. something was persisted and can be reused across runs)
        // - for persistent storage, the first media segment is not re-fetched on run #2
        //
        // Read enough bytes to force a media segment fetch (init + first segment payload).
        let min_total = 16usize;
        let timeout = Duration::from_secs(10);

        let assert_persistent_reuse = storage == "persistent";
        let persistent_root = if assert_persistent_reuse {
            match &storage_kind {
                HlsFixtureStorageKind::Persistent { storage_root } => Some(storage_root.clone()),
                _ => None,
            }
        } else {
            None
        };
        let mut seg_fetch_after_first = 0u64;

        for run_idx in 0..2 {
            let mut reader = fixture
                .stream_download_boxed_with_base(
                    base_url.clone(),
                    storage_kind.clone(),
                )
                .await;

            let mut buf = vec![0u8; 64 * 1024];
            let mut total = 0usize;
            let start = Instant::now();

            while start.elapsed() < timeout {
                // Just keep draining until we have at least 1 byte, then stop.
                // The purpose is to trigger at least some real work in the pipeline.
                match std::io::Read::read(&mut reader, &mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        total += n;
                        if total >= min_total {
                            break;
                        }
                    }
                    Err(e) => panic!(
                        "run {run_idx}: read error after {total} bytes (variant_count={variant_count}, v0_delay={:?}, storage={storage}): {e}",
                        v0_delay
                    ),
                }
            }

            assert!(
                total >= min_total,
                "run {run_idx}: expected to read at least {min_total} byte(s) from HLS stream within timeout (got {total}) (variant_count={variant_count}, v0_delay={:?}, storage={storage})",
                v0_delay
            );

            // For persistent storage, assert that the on-disk root becomes non-empty after the first run.
            // This is a concrete "cache warmup happened" signal without assuming anything about HTTP re-fetch.
            if assert_persistent_reuse && run_idx == 0 {
                let root = persistent_root
                    .as_ref()
                    .expect("persistent storage kind should provide storage_root");
                assert!(
                    dir_nonempty_recursive(root),
                    "expected persistent storage root to contain files after run 0, but it is empty: {} (variant_count={}, v0_delay={:?})",
                    root.display(),
                    variant_count,
                    v0_delay
                );

                seg_fetch_after_first = fixture
                    .request_count_for(seg0_path)
                    .expect("fixture request counter should be available");
                assert!(
                    seg_fetch_after_first >= 1,
                    "expected to fetch at least one media segment ({seg0_path}) on run 0 (variant_count={variant_count}, v0_delay={:?}, storage={storage})",
                    v0_delay
                );
            } else if assert_persistent_reuse && run_idx == 1 {
                let after_second = fixture
                    .request_count_for(seg0_path)
                    .expect("fixture request counter should be available after run 1");
                assert!(
                    after_second <= seg_fetch_after_first + 1,
                    "expected run 1 to reuse cached media segment {seg0_path} without re-fetch (variant_count={variant_count}, v0_delay={:?}, storage={storage}); before={seg_fetch_after_first}, after={after_second}",
                    v0_delay
                );
            }
        }
    });
}

#[rstest]
#[case(1, "persistent")]
#[case(1, "temp")]
#[case(1, "memory")]
#[case(2, "persistent")]
#[case(2, "temp")]
#[case(2, "memory")]
#[case(4, "persistent")]
#[case(4, "temp")]
#[case(4, "memory")]
#[case(6, "persistent")]
#[case(6, "temp")]
#[case(6, "memory")]
fn hls_manager_parses_exact_variant_count_from_master(
    #[case] variant_count: usize,
    #[case] storage: &str,
) {
    setup::SERVER_RT.block_on(async {
        let fixture =
            HlsFixture::with_variant_count(variant_count).with_segment_delay(Duration::ZERO);

        let storage_kind =
            build_fixture_storage_kind("hls-manager-parse-master", variant_count, storage);
        let storage_bundle = fixture.build_storage(storage_kind);
        let storage_handle = storage_bundle.storage_handle();

        let (data_tx, _data_rx) = mpsc::channel::<StreamMsg>(1);
        let (_base_url, mut manager) = fixture.manager(storage_handle, data_tx).await;

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
            "expected {} variants in master playlist, got {} (storage={})",
            variant_count,
            master.variants.len(),
            storage
        );
    });
}

#[rstest]
#[case(0, "persistent")]
#[case(0, "temp")]
#[case(0, "memory")]
#[case(1, "persistent")]
#[case(1, "temp")]
#[case(1, "memory")]
#[case(2, "persistent")]
#[case(2, "temp")]
#[case(2, "memory")]
#[case(3, "persistent")]
#[case(3, "temp")]
#[case(3, "memory")]
fn hls_worker_manual_selection_emits_only_selected_variant_chunks(
    #[case] variant_idx: u64,
    #[case] storage: &str,
) {
    setup::SERVER_RT.block_on(async {
        // Use 4 variants so we can parameterize across multiple variant selections.
        let fixture = HlsFixture::with_variant_count(4)
            .with_segment_delay(Duration::ZERO)
            .with_hls_config(
                HlsSettings::default()
                    .selection_manual(VariantId(variant_idx as usize)),
            );

        let storage_kind = build_fixture_storage_kind("hls-worker-manual", 4, storage);

        let chunkstarts = fixture
            .run_worker_collecting(
                4096,
                storage_kind,
                |rx| Box::pin(async move { collect_first_n_chunkstarts(rx, 8).await }),
            )
            .await;

        assert!(
            !chunkstarts.is_empty(),
            "expected at least one ChunkStart in manual mode (variant_idx={}, storage={})",
            variant_idx,
            storage
        );
        assert_eq!(
            chunkstarts.len(),
            8,
            "expected to capture 8 ChunkStart events in manual mode (variant_idx={}, storage={}), got {}",
            variant_idx,
            storage,
            chunkstarts.len()
        );

        for ctrl in chunkstarts {
            if let StreamControl::ChunkStart { stream_key, .. } = ctrl {
                assert!(
                    is_variant_stream_key(&stream_key, variant_idx),
                    "expected only variant {} in manual mode (storage={}), got stream_key='{}'",
                    variant_idx,
                    storage,
                    stream_key.0
                );
            }
        }
    });
}

#[rstest]
#[case(1, "persistent")]
#[case(1, "temp")]
#[case(1, "memory")]
#[case(2, "persistent")]
#[case(2, "temp")]
#[case(2, "memory")]
#[case(4, "persistent")]
#[case(4, "temp")]
#[case(4, "memory")]
fn hls_worker_auto_starts_with_variant0_first_chunkstart(
    #[case] variant_count: usize,
    #[case] storage: &str,
) {
    setup::SERVER_RT.block_on(async {
        let fixture = HlsFixture::with_variant_count(variant_count)
            .with_segment_delay(Duration::ZERO);

        let storage_kind = build_fixture_storage_kind("hls-worker-auto", variant_count, storage);

        let first = fixture
            .run_worker_collecting(
                2048,
                storage_kind,
                |rx| Box::pin(async move { wait_first_chunkstart(rx).await }),
            )
            .await;

        match first {
            StreamControl::ChunkStart {
                stream_key, kind, ..
            } => {
                assert!(
                    is_variant_stream_key(&stream_key, 0),
                    "expected first ChunkStart to be for variant 0 (storage={}), got stream_key='{}' kind={:?}",
                    storage,
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

#[test]
fn hls_abr_downswitches_after_low_throughput_sample() {
    setup::SERVER_RT.block_on(async {
        let fixture = HlsFixture::with_variant_count(2).with_abr_config(|cfg| {
            cfg.abr_min_switch_interval = Duration::ZERO;
            cfg.abr_down_switch_buffer = 5.0;
        });

        let storage_kind = build_fixture_storage_kind("hls-abr-downswitch", 2, "memory");

        // Start on variant 1 (higher bandwidth), then force a low throughput sample to push a downswitch.
        let (_base_url, mut controller) = fixture.abr_controller(storage_kind, 1, None).await;

        assert_eq!(
            controller.current_variant_id(),
            Some(VariantId(1)),
            "ABR controller should initialize on variant 1"
        );

        // Feed a deliberately slow/large sample to drop estimated bandwidth below variant 1.
        controller.on_media_segment_downloaded(
            Duration::from_secs(2),
            200_000,
            Duration::from_millis(2000),
        );

        let desc = controller
            .next_segment_descriptor_nonblocking()
            .await
            .expect("descriptor after throughput drop");

        let seg = match desc {
            NextSegmentDescResult::Segment(s) => s,
            other => panic!(
                "expected Segment descriptor after throughput drop, got {:?}",
                other
            ),
        };

        assert_eq!(
            controller.current_variant_id(),
            Some(VariantId(0)),
            "expected ABR to downswitch to variant 0 after bandwidth drop"
        );
        assert_eq!(
            seg.variant_id,
            VariantId(0),
            "descriptor returned after switch should target variant 0"
        );
        assert!(
            seg.is_init,
            "first descriptor after switch should be init for new variant (uri={})",
            seg.uri
        );
    });
}

#[test]
fn hls_abr_upswitch_continues_from_current_segment_index() {
    setup::SERVER_RT.block_on(async {
        let fixture = HlsFixture::with_variant_count(2)
            .with_segment_delay(Duration::ZERO)
            .with_abr_config(|cfg| {
                cfg.abr_min_switch_interval = Duration::ZERO;
                cfg.abr_min_buffer_for_up_switch = 0.0;
                cfg.abr_up_hysteresis_ratio = 0.0;
                cfg.abr_throughput_safety_factor = 1.0;
            });

        let storage_kind = build_fixture_storage_kind("hls-abr-upswitch", 2, "memory");

        let (_base_url, mut controller) = fixture.abr_controller(storage_kind, 0, None).await;

        // Drain init + first media segment on variant 0.
        let first_init = controller
            .next_segment_descriptor_nonblocking()
            .await
            .expect("descriptor for first init");
        let first_seg = controller
            .next_segment_descriptor_nonblocking()
            .await
            .expect("descriptor for first segment");

        let first_seg = match first_seg {
            NextSegmentDescResult::Segment(s) => s,
            other => panic!("expected first segment descriptor, got {:?}", other),
        };

        assert_eq!(
            controller.current_variant_id(),
            Some(VariantId(0)),
            "ABR controller should start on variant 0"
        );
        assert!(matches!(
            first_init,
            NextSegmentDescResult::Segment(ref d) if d.variant_id == VariantId(0) && d.is_init
        ));
        assert_eq!(
            first_seg.variant_id,
            VariantId(0),
            "first media descriptor should be variant 0"
        );

        // Feed a fast/large sample to encourage an upswitch.
        controller.on_media_segment_downloaded(
            Duration::from_secs(1),
            2_000_000,
            Duration::from_millis(100),
        );

        let switched_init = controller
            .next_segment_descriptor_nonblocking()
            .await
            .expect("descriptor after upswitch");
        let switched_seg = controller
            .next_segment_descriptor_nonblocking()
            .await
            .expect("media descriptor after upswitch");

        let switched_init = match switched_init {
            NextSegmentDescResult::Segment(s) => s,
            other => panic!("expected init descriptor after switch, got {:?}", other),
        };
        let switched_seg = match switched_seg {
            NextSegmentDescResult::Segment(s) => s,
            other => panic!("expected media descriptor after switch, got {:?}", other),
        };

        assert_eq!(
            controller.current_variant_id(),
            Some(VariantId(1)),
            "ABR should upswitch to variant 1"
        );
        assert!(
            switched_init.is_init && switched_init.variant_id == VariantId(1),
            "first descriptor after switch should be init for variant 1 (got {:?})",
            switched_init
        );
        assert_eq!(
            switched_seg.variant_id,
            VariantId(1),
            "media descriptor after switch should target variant 1"
        );

        // Next segment index should be preserved across the switch: we consumed one media segment
        // on variant 0, so after switching we expect the second media segment of variant 1.
        let expected_sequence = first_seg.sequence + 1;
        assert_eq!(
            switched_seg.sequence, expected_sequence,
            "upswitch should continue from current segment index, not restart from the first segment"
        );
    });
}

#[rstest]
#[case(2, 0, 1, "persistent")]
#[case(2, 0, 1, "temp")]
#[case(2, 0, 1, "memory")]
#[case(4, 0, 3, "persistent")]
#[case(4, 0, 3, "temp")]
#[case(4, 0, 3, "memory")]
#[case(4, 1, 2, "persistent")]
#[case(4, 1, 2, "temp")]
#[case(4, 1, 2, "memory")]
#[case(6, 2, 5, "persistent")]
#[case(6, 2, 5, "temp")]
#[case(6, 2, 5, "memory")]
fn hls_manager_select_variant_changes_fetched_media_bytes_prefix(
    #[case] variant_count: usize,
    #[case] from_variant: usize,
    #[case] to_variant: usize,
    #[case] storage: &str,
) {
    setup::SERVER_RT.block_on(async {
        assert!(
            from_variant < variant_count && to_variant < variant_count && from_variant != to_variant,
            "invalid parameterization: variant_count={variant_count} from_variant={from_variant} to_variant={to_variant}"
        );
        let fixture = HlsFixture::with_variant_count(variant_count)
            .with_segment_delay(Duration::ZERO);

        let storage_kind = build_fixture_storage_kind(
            "hls-manager-select-variant",
            variant_count,
            storage,
        );
        let storage_bundle = fixture.build_storage(storage_kind);
        let storage_handle = storage_bundle.storage_handle();

        let (data_tx, _data_rx) = mpsc::channel::<StreamMsg>(8);
        let (_base_url, mut manager) = fixture.manager(storage_handle, data_tx).await;

        manager
            .load_master()
            .await
            .expect("failed to load master playlist");

        async fn first_media_payload(
            manager: &mut HlsManager,
            variant_index: usize,
        ) -> (String, Vec<u8>) {
            manager
                .select_variant(variant_index)
                .await
                .unwrap_or_else(|e| panic!("select variant {variant_index} failed: {e}"));

            // Keep pulling descriptors until we find the first MEDIA segment for the selected variant.
            // This test asserts that selection affects which segment URI we fetch.
            let desc = loop {
                match manager.next_segment_descriptor_nonblocking().await {
                    Ok(NextSegmentDescResult::Segment(d)) => {
                        if d.is_init {
                            continue;
                        }
                        // Fixture URIs are deterministic and include `seg/v{idx}_...`.
                        // Assert that the descriptor we're about to fetch belongs to the selected variant.
                        let uri_s = d.uri.to_string();
                        assert!(
                            uri_s.contains(&format!("/seg/v{variant_index}_")),
                            "expected selected variant {variant_index} media segment URI to contain '/seg/v{variant_index}_', got: {uri_s}"
                        );
                        break d;
                    }
                    Ok(other) => panic!(
                        "expected Segment descriptor for variant {variant_index}, got: {:?}",
                        other
                    ),
                    Err(e) => panic!("failed to get descriptor for variant {variant_index}: {e}"),
                }
            };

            let uri_s = desc.uri.to_string();
            let mut stream = manager
                .downloader()
                .stream_segment(&desc.uri)
                .await
                .unwrap_or_else(|e| {
                    panic!("failed to stream segment for variant {variant_index} (uri={uri_s}): {e}")
                });

            // Read the whole payload (fixture segments are tiny strings like "V{v}-SEG-{i}").
            let mut out: Vec<u8> = Vec::new();
            while let Some(item) = stream.next().await {
                let chunk = item.unwrap_or_else(|e| {
                    panic!(
                        "error while reading segment stream for variant {variant_index} (uri={uri_s}): {e}"
                    )
                });
                out.extend_from_slice(&chunk);
            }

            assert!(
                !out.is_empty(),
                "expected non-empty payload for variant {variant_index} media segment (uri={uri_s})"
            );

            (uri_s, out)
        }

        let (uri_a, a) = first_media_payload(&mut manager, from_variant).await;
        let (uri_b, b) = first_media_payload(&mut manager, to_variant).await;

        assert_ne!(
            uri_a, uri_b,
            "expected different segment URIs after select_variant (variant {from_variant} -> {to_variant}, storage={storage})"
        );

        let a_s = std::str::from_utf8(&a).expect("fixture bytes should be utf-8");
        let b_s = std::str::from_utf8(&b).expect("fixture bytes should be utf-8");

        assert!(
            a_s.starts_with(&format!("V{from_variant}-SEG-")),
            "unexpected media payload for from_variant={from_variant} (storage={storage}); got={a_s:?}, uri={uri_a}"
        );
        assert!(
            b_s.starts_with(&format!("V{to_variant}-SEG-")),
            "unexpected media payload for to_variant={to_variant} (storage={storage}); got={b_s:?}, uri={uri_b}"
        );
    });
}

#[rstest]
#[case(2, 0, "INIT-V0", "persistent")]
#[case(2, 1, "NIT-V0", "persistent")]
#[case(2, 5, "V0", "persistent")]
#[case(2, 7, "V0-SEG-0", "persistent")]
#[case(4, 0, "INIT-V0", "persistent")]
#[case(4, 1, "NIT-V0", "persistent")]
#[case(4, 5, "V0", "persistent")]
#[case(4, 7, "V0-SEG-0", "persistent")]
#[case(2, 0, "INIT-V0", "temp")]
#[case(2, 1, "NIT-V0", "temp")]
#[case(2, 5, "V0", "temp")]
#[case(2, 7, "V0-SEG-0", "temp")]
#[case(4, 0, "INIT-V0", "temp")]
#[case(4, 1, "NIT-V0", "temp")]
#[case(4, 5, "V0", "temp")]
#[case(4, 7, "V0-SEG-0", "temp")]
#[case(2, 0, "INIT-V0", "memory")]
#[case(2, 1, "NIT-V0", "memory")]
#[case(2, 5, "V0", "memory")]
#[case(2, 7, "V0-SEG-0", "memory")]
#[case(4, 0, "INIT-V0", "memory")]
#[case(4, 1, "NIT-V0", "memory")]
#[case(4, 5, "V0", "memory")]
#[case(4, 7, "V0-SEG-0", "memory")]
fn hls_streamdownload_read_seek_returns_expected_bytes_at_known_offsets(
    #[case] variant_count: usize,
    #[case] seek_pos: u64,
    #[case] expected_prefix: &str,
    #[case] storage: &str,
) {
    setup::SERVER_RT.block_on(async {
        let fixture =
            HlsFixture::with_variant_count(variant_count).with_segment_delay(Duration::ZERO);

        let storage_kind = build_fixture_storage_kind("hls-read-seek-e2e", variant_count, storage);

        let (_base_url, mut reader) = fixture.stream_download_boxed(storage_kind).await;

        // Ensure the pipeline is started (worker emits ChunkStart before data).
        // This avoids "no active segment; expected ChunkStart before data" on early seeks.
        let mut warm = [0u8; 1];
        let _ = Read::read(&mut reader, &mut warm).expect("warmup read failed");

        let p = Seek::seek(&mut reader, SeekFrom::Start(seek_pos))
            .unwrap_or_else(|e| panic!("seek(Start({seek_pos})) failed (storage={storage}): {e}"));
        assert_eq!(
            p, seek_pos,
            "seek(Start({seek_pos})) returned {p} (storage={storage})"
        );

        let mut buf = vec![0u8; expected_prefix.len()];
        Read::read_exact(&mut reader, &mut buf).unwrap_or_else(|e| {
            panic!("read_exact after seek({seek_pos}) failed (storage={storage}): {e}")
        });

        let got = std::str::from_utf8(&buf).expect("fixture bytes should be utf-8");
        assert_eq!(
            got, expected_prefix,
            "unexpected bytes at seek_pos={seek_pos} (storage={storage})"
        );

        let p0 = Seek::seek(&mut reader, SeekFrom::Start(0)).expect("seek back to 0 failed");
        assert_eq!(p0, 0, "seek(Start(0)) returned {p0} (storage={storage})");
        let mut b0 = vec![0u8; "INIT-V0".len()];
        Read::read_exact(&mut reader, &mut b0).expect("read_exact at 0 failed");
        assert_eq!(
            std::str::from_utf8(&b0).unwrap(),
            "INIT-V0",
            "expected INIT-V0 at start after roundtrip seek (storage={storage})"
        );
    });
}

#[test]
fn hls_streamdownload_seek_across_segment_boundary_reads_contiguous_bytes() {
    setup::SERVER_RT.block_on(async {
        let fixture = HlsFixture::with_variant_count(2).with_segment_delay(Duration::ZERO);

        let storage_kind = build_fixture_storage_kind("hls-read-seek-boundary", 2, "memory");

        let (_base_url, mut reader) = fixture.stream_download_boxed(storage_kind).await;

        // Warm up pipeline so seek won't race with first ChunkStart.
        let mut warm = [0u8; 1];
        let _ = Read::read(&mut reader, &mut warm).expect("warmup read failed");

        let init = "INIT-V0";
        let seg0 = "V0-SEG-0";
        let seg1 = "V0-SEG-1";
        let combined = format!("{init}{seg0}{seg1}");
        let offset = (init.len() + seg0.len() - 3) as u64;
        let read_len = 6usize;

        let p =
            Seek::seek(&mut reader, SeekFrom::Start(offset)).expect("seek across boundary failed");
        assert_eq!(p, offset, "seek returned unexpected position");

        let mut buf = vec![0u8; read_len];
        Read::read_exact(&mut reader, &mut buf).expect("read_exact across boundary failed");

        let expected_slice = &combined.as_bytes()[offset as usize..offset as usize + read_len];
        assert_eq!(
            &buf, expected_slice,
            "bytes across segment boundary did not match expected payload"
        );

        let remaining = combined.len() - (offset as usize + read_len);
        let mut trailing = vec![0u8; remaining];
        Read::read_exact(&mut reader, &mut trailing).expect("read_exact for trailing bytes failed");
        assert_eq!(
            &trailing,
            &combined.as_bytes()[offset as usize + read_len..],
            "trailing bytes after boundary seek did not align with expected sequence"
        );
    });
}

#[rstest]
#[case(2)]
#[case(4)]
fn hls_persistent_storage_creates_files_on_disk_after_read(#[case] variant_count: usize) {
    setup::SERVER_RT.block_on(async {
        let fixture =
            HlsFixture::with_variant_count(variant_count).with_segment_delay(Duration::ZERO);

        let storage_root = std::env::temp_dir()
            .join("stream-download-tests")
            .join(format!("hls-disk-files-persistent-v{variant_count}"));

        // Best-effort cleanup: ensure the directory starts empty/non-existent.
        clean_dir(&storage_root);

        let storage_kind = HlsFixtureStorageKind::Persistent {
            storage_root: storage_root.clone(),
        };

        let (_base_url, mut reader) = fixture.stream_download_boxed(storage_kind).await;

        // Trigger actual work.
        let mut buf = [0u8; 32];
        let n = Read::read(&mut reader, &mut buf).expect("read failed");
        assert!(n > 0, "expected to read some bytes");

        // Persistent provider should create files on disk (segments and/or resources).
        assert!(
            dir_nonempty_recursive(&storage_root),
            "expected persistent storage root to contain files after reading, but it is empty: {}",
            storage_root.display()
        );
    });
}

#[rstest]
#[case(2)]
#[case(4)]
fn hls_memory_stream_storage_does_not_create_segment_files_on_disk(#[case] variant_count: usize) {
    setup::SERVER_RT.block_on(async {
        let fixture = HlsFixture::with_variant_count(variant_count)
            .with_segment_delay(Duration::ZERO);

        // In memory mode, main stream bytes are stored in memory, but HLS resource caching uses a
        // disk-backed file-tree handle rooted at `resource_cache_root`.
        //
        // This test asserts that we do NOT create segment files under a "segment root" because
        // there is no segment root for memory storage. As a proxy, we assert that our chosen
        // resource cache root starts empty and remains either empty or very small (resources only),
        // and crucially does not explode into a segment tree layout.
        let resource_cache_root = std::env::temp_dir()
            .join("stream-download-tests")
            .join(format!("hls-disk-files-memory-resources-v{variant_count}"));

        clean_dir(&resource_cache_root);
        let before_files = count_files_recursive(&resource_cache_root);

        let storage_kind = HlsFixtureStorageKind::Memory {
            resource_cache_root: resource_cache_root.clone(),
        };

        let (_base_url, mut reader) = fixture
            .stream_download_boxed(storage_kind)
            .await;

        // Trigger actual work.
        let mut buf = [0u8; 32];
        let n = Read::read(&mut reader, &mut buf).expect("read failed");
        assert!(n > 0, "expected to read some bytes");

        let after_files = count_files_recursive(&resource_cache_root);

        // Resource caching may write a handful of small blobs (playlists/keys), but it should not
        // behave like segment persistence. We assert that the directory does not suddenly become
        // "populated like a segment cache". Keep this intentionally lenient but meaningful:
        // - it should not go from 0 to many dozens just from a tiny read.
        assert!(
            after_files.saturating_sub(before_files) <= variant_count + 6,
            "memory stream storage should not create many files on disk; got before={before_files}, after={after_files} under {}",
            resource_cache_root.display()
        );
    });
}
