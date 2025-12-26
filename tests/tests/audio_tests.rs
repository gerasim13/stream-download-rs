use std::collections::HashMap;
use std::time::Duration;

use fixtures::audio::AudioFixture;
use fixtures::setup::{SERVER_RT, server_addr};
use rstest::rstest;
use stream_download_audio::AudioSettings;

mod fixtures;

#[rstest]
fn audio_hls_decodes_real_assets_from_local_server() {
    SERVER_RT.block_on(async move {
        let mut stream = AudioFixture::audio_stream_hls_real_assets(
            server_addr(),
            AudioSettings::default(),
            stream_download_hls::HlsSettings::default(),
            None,
            None,
        )
        .await;

        let obs = AudioFixture::drive_and_observe(&mut stream, Duration::from_secs(20), 8192).await;

        assert!(
            obs.error_message.is_none(),
            "unexpected audio pipeline error while decoding HLS assets: {:?} (last_event={:?})",
            obs.error_message,
            obs.last_event
        );

        assert!(
            obs.saw_format_changed,
            "expected to observe PlayerEvent::FormatChanged while decoding HLS assets (last_event={:?})",
            obs.last_event
        );

        assert!(
            obs.total_samples >= 8192,
            "expected to decode at least 8192 PCM samples from HLS assets, got {} (last_event={:?})",
            obs.total_samples,
            obs.last_event
        );
    });
}

#[rstest]
fn audio_hls_abr_variant_switch_emits_event_and_pcm_continues() {
    SERVER_RT.block_on(async move {
        let mut stream = AudioFixture::audio_stream_hls_real_assets(
            server_addr(),
            AudioSettings::default(),
            stream_download_hls::HlsSettings::default(),
            None,
            None,
        )
        .await;

        // Primary signal: decode works and PCM continues.
        // ABR switching is a secondary, "best effort" signal unless we introduce deterministic
        // throughput shaping at the fixture server layer.
        let obs =
            AudioFixture::drive_and_observe(&mut stream, Duration::from_secs(30), 65536).await;

        assert!(
            obs.error_message.is_none(),
            "unexpected audio pipeline error while decoding HLS assets: {:?} (last_event={:?})",
            obs.error_message,
            obs.last_event
        );

        assert!(
            obs.saw_format_changed,
            "expected to observe PlayerEvent::FormatChanged while decoding HLS assets (last_event={:?})",
            obs.last_event
        );

        assert!(
            obs.total_samples >= 8192,
            "expected to decode at least 8192 PCM samples from HLS assets, got {} (last_event={:?})",
            obs.total_samples,
            obs.last_event
        );

        // Best-effort ABR signal: do not fail if no switch happens.
        let _ = obs.saw_variant_switched;
    });
}

#[rstest]
fn audio_hls_abr_variant_switch_is_deterministic_under_throughput_shaping() {
    SERVER_RT.block_on(async move {
        // NOTE: This test is intentionally strict and is expected to FAIL until we implement
        // deterministic throughput shaping in the fixture server (e.g., per-path throttling for
        // one variant's playlist/segments).
        //
        // Once the server can reliably degrade one variant (latency/throttle), ABR should switch
        // and emit PlayerEvent::VariantSwitched.

        // Degrade the lowest-quality AAC variant (`index-slq-a1.m3u8`) by adding artificial per-file delays.
        //
        // Keys are paths relative to the assets root (served by `ServeDir`), without a leading `/`.
        // Example: `hls/index-slq-a1.m3u8`, `hls/segment-1-slq-a1.m4s`.
        //
        // NOTE: This is a TDD contract: it will have no effect until the shared assets server
        // (`tests/tests/fixtures/setup.rs`) implements per-path delay injection.
        let slow_variant = Some(HashMap::from([
            // Variant playlist (lowest quality AAC)
            ("hls/index-slq-a1.m3u8".to_string(), Duration::from_millis(1500)),
            // Init segment for that variant
            ("hls/init-slq-a1.mp4".to_string(), Duration::from_millis(1500)),
            // A few early media segments — enough to reliably impact ABR heuristics
            ("hls/segment-1-slq-a1.m4s".to_string(), Duration::from_millis(1500)),
            ("hls/segment-2-slq-a1.m4s".to_string(), Duration::from_millis(1500)),
            ("hls/segment-3-slq-a1.m4s".to_string(), Duration::from_millis(1500)),
            ("hls/segment-4-slq-a1.m4s".to_string(), Duration::from_millis(1500)),
            ("hls/segment-5-slq-a1.m4s".to_string(), Duration::from_millis(1500)),
            ("hls/segment-6-slq-a1.m4s".to_string(), Duration::from_millis(1500)),
        ]));

        let mut stream = AudioFixture::audio_stream_hls_real_assets(
            server_addr(),
            AudioSettings::default(),
            stream_download_hls::HlsSettings::default(),
            None,
            slow_variant,
        )
        .await;

        let obs =
            AudioFixture::drive_and_observe(&mut stream, Duration::from_secs(30), 65536).await;

        assert!(
            obs.error_message.is_none(),
            "unexpected audio pipeline error while decoding HLS assets: {:?} (last_event={:?})",
            obs.error_message,
            obs.last_event
        );

        assert!(
            obs.saw_format_changed,
            "expected to observe PlayerEvent::FormatChanged while decoding HLS assets (last_event={:?})",
            obs.last_event
        );

        assert!(
            obs.total_samples >= 8192,
            "expected to decode at least 8192 PCM samples from HLS assets, got {} (last_event={:?})",
            obs.total_samples,
            obs.last_event
        );

        assert!(
            obs.saw_variant_switched,
            "expected deterministic ABR variant switch under throughput shaping, but no PlayerEvent::VariantSwitched observed (last_event={:?})",
            obs.last_event
        );
    });
}
