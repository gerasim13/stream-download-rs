use std::error::Error;

use stream_download::source::DecodeError;
use stream_download::storage::temp::TempStorageProvider;
use stream_download::{Settings, StreamDownload};
use stream_download_hls::{
    HlsSettings, HlsStream, HlsStreamParams, SegmentedStorageProvider, VariantId,
};
use tracing::metadata::LevelFilter;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::default()
                .add_directive("stream_download_hls=trace".parse()?)
                .add_directive(LevelFilter::INFO.into()),
        )
        .with_line_number(false)
        .with_file(false)
        .init();

    let url = "https://stream.silvercomet.top/hls/master.m3u8".to_string();
    let manual_variant_idx = 0;
    let settings = HlsSettings::default().selection_manual(VariantId(manual_variant_idx));
    let params = HlsStreamParams::new(url, settings);

    let reader = match StreamDownload::new::<HlsStream>(
        params,
        SegmentedStorageProvider::new(|| TempStorageProvider::default()),
        Settings::default(),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            return Err(e.decode_error().await)?;
        }
    };

    let handle = tokio::task::spawn_blocking(move || {
        let stream_handle = rodio::OutputStreamBuilder::open_default_stream()?;
        let sink = rodio::Sink::connect_new(stream_handle.mixer());
        sink.append(rodio::Decoder::new(reader)?);
        sink.play();
        sink.sleep_until_end();

        Ok::<_, Box<dyn Error + Send + Sync>>(())
    });
    handle.await??;

    Ok(())
}
