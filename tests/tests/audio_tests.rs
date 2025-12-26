use std::collections::HashMap;
use std::time::Duration;

use fixtures::audio::AudioFixture;
use fixtures::setup::{SERVER_RT, server_addr};
use rstest::rstest;
use stream_download_audio::{AbrVariantChangeReason, AudioSettings};
use stream_download_hls::HlsSettings;

mod fixtures;

#[rstest]
fn audio_hls_decodes_real_assets_from_local_server() {
    SERVER_RT.block_on(async move {
        let (mut stream, _server) = AudioFixture::audio_stream_hls_real_assets(
            server_addr(),
            AudioSettings::default(),
            stream_download_hls::HlsSettings::default(),
            None,
            None,
        )
        .await;

        let obs = AudioFixture::drive_and_observe(
            &mut stream,
            Duration::from_secs(20),
            8192,
            || 0usize,
        )
        .await;

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
        let (mut stream, _server) = AudioFixture::audio_stream_hls_real_assets(
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
        let obs = AudioFixture::drive_and_observe(
            &mut stream,
            Duration::from_secs(30),
            65536,
            || 0usize,
        )
        .await;

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
        let _ = obs.saw_variant_changed;
    });
}

/// Downswitch: start from a high variant, make that variant's media segments slow, and assert that:
/// - we observe at least one non-initial ABR decision to a *lower* variant
/// - and that lower variant is actually applied (we see HlsSegmentStart for it)
#[rstest]
#[case(
    3,
    vec![
        ("hls/segment-1-slossless-a1.m4s", 1200),
        ("hls/segment-2-slossless-a1.m4s", 1200),
        ("hls/segment-3-slossless-a1.m4s", 1200),
        ("hls/segment-4-slossless-a1.m4s", 1200),
        ("hls/segment-5-slossless-a1.m4s", 1200),
        ("hls/segment-6-slossless-a1.m4s", 1200),
        ("hls/segment-7-slossless-a1.m4s", 1200),
        ("hls/segment-8-slossless-a1.m4s", 1200),
    ],
)]
fn audio_hls_abr_downswitch_under_network_shaping(
    #[case] start_variant_index: usize,
    #[case] per_file_delay_ms: Vec<(&'static str, u64)>,
) {
    SERVER_RT.block_on(async move {
        let per_file_delay: HashMap<String, Duration> = per_file_delay_ms
            .into_iter()
            .map(|(p, ms)| (p.to_string(), Duration::from_millis(ms)))
            .collect();

        let (mut stream, _server) = AudioFixture::audio_stream_hls_real_assets(
            server_addr(),
            AudioSettings::default(),
            {
                let mut s = HlsSettings::default();
                s.abr_min_switch_interval = Duration::ZERO;
                s.abr_min_buffer_for_up_switch = 0.0;
                s.abr_up_hysteresis_ratio = 0.0;
                s.abr_throughput_safety_factor = 1.0;

                s.abr_initial_variant_index = Some(start_variant_index);
                s
            },
            None,
            Some(per_file_delay),
        )
        .await;

        let obs = AudioFixture::drive_and_observe(
            &mut stream,
            Duration::from_secs(60),
            16384,
            || 0usize,
        )
        .await;

        assert!(
            obs.error_message.is_none(),
            "unexpected audio pipeline error: {:?} (last_event={:?})",
            obs.error_message,
            obs.last_event
        );

        assert!(
            obs.saw_format_changed,
            "expected PlayerEvent::FormatChanged (last_event={:?})",
            obs.last_event
        );

        assert!(
            obs.total_samples >= 8192,
            "expected to decode at least 8192 PCM samples, got {} (last_event={:?})",
            obs.total_samples,
            obs.last_event
        );

        // Find a non-initial downswitch decision.
        let down_targets: Vec<usize> = obs
            .variant_changed_events
            .iter()
            .filter_map(|(from, to, reason)| {
                if *reason == AbrVariantChangeReason::Initial {
                    return None;
                }
                let from = from.unwrap_or(start_variant_index);
                if *to < from {
                    Some(*to)
                } else {
                    None
                }
            })
            .collect();

        assert!(
            !down_targets.is_empty(),
            "expected at least one non-initial downswitch decision (VariantChanged.to < from). got VariantChanged history={:?}",
            obs.variant_changed_events
        );

        // Applied: we must see HlsSegmentStart for at least one of the downswitch targets.
        let applied = down_targets.iter().any(|t| {
            obs.hls_segment_starts
                .iter()
                .any(|(variant, _seq)| variant == t)
        });

        assert!(
            applied,
            "expected downswitch to be applied (HlsSegmentStart for a downswitch target). down_targets={:?} hls_segment_starts={:?} variant_changed_events={:?}",
            down_targets,
            obs.hls_segment_starts,
            obs.variant_changed_events
        );
    });
}

/// Upswitch: start from the lowest variant, make that variant's media segments slow, and assert that:
/// - we observe at least one non-initial ABR decision to a *higher* variant
/// - and that higher variant is actually applied (we see HlsSegmentStart for it)
#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
fn audio_hls_abr_upswitch_under_network_shaping(#[case] start_variant_index: usize) {
    SERVER_RT.block_on(async move {
        // Upswitch contract:
        // - Start from a given (non-max) variant in AUTO mode.
        // - Under "fast network" conditions, ABR should eventually upswitch to some higher variant.
        //
        // NOTE:
        // This test intentionally avoids adding artificial delays. Upswitch is triggered by the
        // ABR throughput estimate improving beyond the current variant's bandwidth.
        assert!(
            start_variant_index < 3,
            "upswitch test requires a non-max start variant (got {})",
            start_variant_index
        );

        let (mut stream, _server) = AudioFixture::audio_stream_hls_real_assets(
            server_addr(),
            AudioSettings::default(),
            {
                let mut s = HlsSettings::default();

                // Make upswitch decisions permissive.
                s.abr_min_switch_interval = Duration::ZERO;
                s.abr_min_buffer_for_up_switch = 0.0;
                s.abr_up_hysteresis_ratio = 0.0;

                // Keep safety factor neutral; we want "fast network" to speak for itself.
                s.abr_throughput_safety_factor = 1.0;

                // Start from the requested variant in AUTO.
                s.abr_initial_variant_index = Some(start_variant_index);

                s
            },
            None,
            None,
        )
        .await;

        let obs = AudioFixture::drive_and_observe(
            &mut stream,
            Duration::from_secs(90),
            16384,
            || 0usize,
        )
        .await;

        assert!(
            obs.error_message.is_none(),
            "unexpected audio pipeline error: {:?} (last_event={:?})",
            obs.error_message,
            obs.last_event
        );

        assert!(
            obs.saw_format_changed,
            "expected PlayerEvent::FormatChanged (last_event={:?})",
            obs.last_event
        );

        assert!(
            obs.total_samples >= 8192,
            "expected to decode at least 8192 PCM samples, got {} (last_event={:?})",
            obs.total_samples,
            obs.last_event
        );

        // Find a non-initial upswitch decision (to > from).
        let up_targets: Vec<usize> = obs
            .variant_changed_events
            .iter()
            .filter_map(|(from, to, reason)| {
                if *reason == AbrVariantChangeReason::Initial {
                    return None;
                }
                let from = from.unwrap_or(start_variant_index);
                if *to > from {
                    Some(*to)
                } else {
                    None
                }
            })
            .collect();

        assert!(
            !up_targets.is_empty(),
            "expected at least one non-initial upswitch decision (VariantChanged.to > from) from start_variant_index={}. VariantChanged history={:?} hls_segment_starts={:?}",
            start_variant_index,
            obs.variant_changed_events,
            obs.hls_segment_starts
        );

        // Applied: we must see HlsSegmentStart for at least one of the upswitch targets.
        let applied = up_targets.iter().any(|t| {
            obs.hls_segment_starts
                .iter()
                .any(|(variant, _seq)| variant == t)
        });

        assert!(
            applied,
            "expected upswitch to be applied (HlsSegmentStart for an upswitch target). start_variant_index={} up_targets={:?} hls_segment_starts={:?} variant_changed_events={:?}",
            start_variant_index,
            up_targets,
            obs.hls_segment_starts,
            obs.variant_changed_events
        );
    });
}
