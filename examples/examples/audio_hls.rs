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
                .add_directive("stream_download_audio=trace".parse()?)
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

    // Enable ABR with default settings
    let hls_settings = HlsSettings::default();

    println!("Creating AudioStream (decoder will start immediately)...");
    let mut stream =
        AudioStream::new_hls(url, Some(storage_root), audio_settings, hls_settings).await;
    println!("AudioStream created! Decoder loop is now running.");

    // Subscribe to player events
    let events = stream.subscribe_events();
    tokio::spawn(async move {
        loop {
            match events.recv() {
                Ok(event) => {
                    println!("[EVENT] {:?}", event);
                }
                Err(_) => break,
            }
        }
    });

    // Setup rodio output
    println!("Setting up Rodio output...");
    let stream_handle =
        OutputStreamBuilder::open_default_stream().expect("open default audio stream");
    let sink = Sink::connect_new(&stream_handle.mixer());
    println!("Rodio output ready.");

    // Adapt AudioStream into a rodio Source and play
    println!("Creating RodioSourceAdapter (this will start reading PCM)...");
    let source = RodioSourceAdapter::new(stream);
    println!("RodioSourceAdapter created!");
    sink.set_volume(0.01);
    sink.append(source);
    sink.play();

    // Wait for some time to let audio play and ABR switching happen
    // TODO: implement proper stream end detection in RodioSourceAdapter
    tokio::time::sleep(std::time::Duration::from_secs(60)).await;

    Ok(())
}
