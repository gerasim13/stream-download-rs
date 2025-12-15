use std::error::Error;
use std::time::Duration;

use rodio::{OutputStreamBuilder, Sink};
use stream_download::source::DecodeError;
use stream_download::storage::temp::TempStorageProvider;
use stream_download::{Settings, StreamDownload};
use stream_download_hls::{HlsSettings, HlsStream, HlsStreamParams, VariantId};
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Tracing setup: enable HLS crate logs by default; can be overridden by RUST_LOG
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::default()
                .add_directive("stream_download_hls=debug".parse().unwrap())
                .add_directive("stream_download=info".parse().unwrap())
        }))
        .with_line_number(false)
        .with_file(false)
        .init();

    // Fixed demo parameters.
    let url = "https://stream.silvercomet.top/hls/master.m3u8".to_string();
    let seek_after = Duration::from_secs(5);
    let seek_forward = Duration::from_secs(120);

    // Basic HLS settings: pick the first variant for determinism in the example
    let settings = HlsSettings::default().selection_manual(VariantId(0));
    let params = HlsStreamParams::new(url, settings);

    // Create a blocking reader over the HLS stream via StreamDownload
    let reader = match StreamDownload::new::<HlsStream>(
        params,
        TempStorageProvider::default(),
        Settings::default(),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            // Decode the error text to make diagnostics friendlier
            return Err::<(), _>(e.decode_error().await.into());
        }
    };

    // Run audio playback on a blocking thread. Seek is performed via Sink::try_seek.
    tokio::task::spawn_blocking(move || -> Result<(), Box<dyn Error + Send + Sync>> {
        // Create audio output
        let stream_handle = OutputStreamBuilder::open_default_stream()?;
        // Initial sink + decoder
        let sink = Sink::connect_new(stream_handle.mixer());
        sink.append(rodio::Decoder::new(reader)?);
        sink.play();

        // After 2 seconds, attempt to seek forward by 2 seconds using rodio's duration-based seek.
        std::thread::sleep(seek_after);
        let current = sink.get_pos();
        let target = current + seek_forward;
        if let Err(e) = sink.try_seek(target) {
            tracing::warn!("Sink::try_seek failed: {:?}", e);
        } else {
            tracing::info!("Sink::try_seek to {:?}", target);
        }

        sink.sleep_until_end();
        Ok(())
    })
    .await??;

    Ok(())
}
