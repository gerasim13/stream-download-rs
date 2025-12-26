//! Audio integration tests.
//!
//! Style: matches `tests/tests/hls_tests.rs`
//! - Uses the shared local assets server runtime (`fixtures::SERVER_RT`).
//! - No external network.
//! - Avoids `#[tokio::test]` and uses `SERVER_RT.block_on`.
//!
//! These tests validate the *audio layer* (decode -> PCM, events) over the real HLS assets shipped
//! under `assets/hls/` and served by the shared `ServeDir` server.

use std::time::Duration;

use rstest::rstest;

use stream_download_audio::{AudioSettings, AudioStream, PlayerEvent};

use fixtures::{SERVER_RT, server_addr};

mod fixtures;

#[rstest]
fn audio_hls_decodes_real_assets_from_local_server() {
    SERVER_RT.block_on(async move {
        // These assets are served by the existing axum ServeDir on `../assets`.
        // See `tests/tests/fixtures/setup.rs` for server setup and ASSETS root.
        let url = format!("http://{}/hls/master.m3u8", server_addr())
            .parse()
            .expect("failed to parse HLS master URL");

        let audio_settings = AudioSettings::default();
        let hls_settings = stream_download_hls::HlsSettings::default();

        let mut stream =
            AudioStream::<stream_download::storage::temp::TempStorageProvider>::new_hls(
                url,
                None,
                audio_settings,
                hls_settings,
            )
            .await;

        // We expect to see a format change event once the decoder is initialized.
        // Also, we should be able to pull at least some PCM samples.
        let events = stream.subscribe_events();
        let hard_deadline = std::time::Instant::now() + Duration::from_secs(20);

        let mut saw_format_changed = false;
        let mut saw_error: Option<String> = None;
        let mut last_event: Option<PlayerEvent> = None;

        let mut pcm = vec![0.0f32; 4096];
        let mut total_samples = 0usize;

        while std::time::Instant::now() < hard_deadline {
            // Drain events opportunistically.
            loop {
                match events.try_recv() {
                    Ok(Some(ev)) => {
                        if matches!(ev, PlayerEvent::FormatChanged { .. }) {
                            saw_format_changed = true;
                        }
                        if let PlayerEvent::Error { message } = &ev {
                            saw_error = Some(message.clone());
                        }
                        last_event = Some(ev);
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            // Pull PCM.
            let n = stream.pop_chunk(&mut pcm);
            if n > 0 {
                total_samples += n;
                // "Enough to prove decode works" without asserting exact durations.
                if total_samples >= 8192 {
                    break;
                }
            } else {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }

        assert!(
            saw_error.is_none(),
            "unexpected audio pipeline error while decoding HLS assets: {:?} (last_event={last_event:?})",
            saw_error
        );

        assert!(
            saw_format_changed,
            "expected to observe PlayerEvent::FormatChanged while decoding HLS assets (last_event={last_event:?})"
        );

        assert!(
            total_samples > 0,
            "expected to decode some PCM samples from HLS assets (last_event={last_event:?})"
        );
    });
}
