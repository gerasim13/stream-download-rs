use std::error::Error;
use std::time::Duration;

use rodio::{OutputStreamBuilder, Sink};
use stream_download::source::DecodeError;
use stream_download::storage::temp::TempStorageProvider;
use stream_download::{Settings, StreamDownload};
use stream_download_hls::SegmentedStorageProvider;
use stream_download_hls::{HlsSettings, HlsStream, HlsStreamParams, VariantId};
use tracing::metadata::LevelFilter;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::default()
                .add_directive("stream_download_hls=trace".parse()?)
                .add_directive("stream_download=info".parse()?)
                .add_directive(LevelFilter::INFO.into()),
        )
        .with_line_number(false)
        .with_file(false)
        .init();

    // Fixed demo parameters.
    let url = "https://stream.silvercomet.top/hls/master.m3u8".to_string();
    let seek_after = Duration::from_secs(5);
    let seek_forward = Duration::from_secs(60);

    // Basic HLS settings: pick the first variant for determinism in the example
    let settings = HlsSettings::default().selection_manual(VariantId(0));
    let params = HlsStreamParams::new(url, settings);

    // Create a blocking reader over the HLS stream via StreamDownload, but store each HLS chunk
    // (init/media segments) separately via SegmentedStorageProvider. This enables correct seeking.
    let reader = match StreamDownload::new::<HlsStream>(
        params,
        SegmentedStorageProvider::new(|| TempStorageProvider::default()),
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
        let stream_handle = OutputStreamBuilder::open_default_stream()?;
        let sink = Sink::connect_new(stream_handle.mixer());
        sink.append(rodio::Decoder::new(reader)?);
        sink.play();

        std::thread::sleep(seek_after);
        let target = sink.get_pos() + seek_forward;
        if let Err(e) = sink.try_seek(target) {
            tracing::warn!("Sink::try_seek forward failed: {:?}", e);
        } else {
            tracing::info!("Sink::try_seek forward to {:?}", target);
        }

        std::thread::sleep(seek_after * 2);
        let target = sink.get_pos() - seek_forward;
        if let Err(e) = sink.try_seek(target) {
            tracing::warn!("Sink::try_seek backward failed: {:?}", e);
        } else {
            tracing::info!("Sink::try_seek backward to {:?}", target);
        }

        sink.sleep_until_end();
        Ok(())
    })
    .await??;

    Ok(())
}
