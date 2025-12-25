use std::env;
use std::error::Error;

use rodio::{OutputStreamBuilder, Sink};
use stream_download_audio::{AudioSettings, AudioStream, RodioSourceAdapter};
use stream_download_hls::HlsSettings;
use tracing::metadata::LevelFilter;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::default()
                .add_directive("stream_download=info".parse()?)
                .add_directive("stream_download_hls=info".parse()?)
                .add_directive("stream_download_audio=info".parse()?)
                .add_directive(LevelFilter::INFO.into()),
        )
        .with_line_number(false)
        .with_file(false)
        .init();

    let url = "https://stream.silvercomet.top/hls/master.m3u8".parse()?;

    // Build audio stream with HLS backend
    // Specify storage root for HLS caching (playlists, keys, segments)
    let storage_root = env::temp_dir().join("stream-download-audio-hls-example");
    
    let audio_settings = AudioSettings::default();
    let hls_settings = HlsSettings::default();
    
    let stream = AudioStream::new_hls(
        url,
        Some(storage_root),
        audio_settings,
        hls_settings,
    )
    .await;

    // Setup rodio output
    let stream_handle =
        OutputStreamBuilder::open_default_stream().expect("open default audio stream");
    let sink = Sink::connect_new(&stream_handle.mixer());

    // Adapt AudioStream into a rodio Source and play
    let source = RodioSourceAdapter::new(stream);
    sink.set_volume(0.03);
    sink.append(source);
    sink.play();
    sink.sleep_until_end();

    Ok(())
}
