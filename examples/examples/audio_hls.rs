use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;

use rodio::{OutputStreamBuilder, Sink};
use stream_download_audio::{
    AbrConfig, AudioOptions, AudioStream, HlsConfig, PlayerEvent, RodioSourceAdapter,
    SelectionMode, VariantId,
};
use tracing::metadata::LevelFilter;
use tracing::trace;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::default()
                .add_directive("stream_download_audio=debug".parse()?)
                .add_directive(LevelFilter::INFO.into()),
        )
        .with_line_number(false)
        .with_file(false)
        .init();

    let url = "https://stream.silvercomet.top/hls/master.m3u8".to_string();
    let manual_variant_idx = 0usize;
    let selection_mode = SelectionMode::Manual(VariantId(0));

    // Build audio stream
    let opts = AudioOptions::default().with_selection_mode(selection_mode);
    let stream = AudioStream::from_hls(
        url.clone(),
        opts,
        HlsConfig::default(),
        AbrConfig::default(),
    )
    .await;

    // Setup rodio output
    let stream_handle =
        OutputStreamBuilder::open_default_stream().expect("open default audio stream");
    let sink = Sink::connect_new(&stream_handle.mixer());

    // Adapt AudioStream into a rodio Source and play.
    let shared = Arc::new(Mutex::new(stream));
    let source = RodioSourceAdapter::new(shared.clone());
    sink.append(source);
    sink.play();
    sink.sleep_until_end();

    Ok(())
}
