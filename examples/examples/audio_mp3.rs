use std::error::Error;

use rodio::{OutputStreamBuilder, Sink};
use stream_download::http::HttpStream;
use stream_download::source::DecodeError;
use stream_download::storage::temp::TempStorageProvider;
use stream_download::{Settings, StreamDownload};
use stream_download_audio::{
    AudioDecodeOptions, AudioDecodeStream, RodioSourceAdapter, TapStorageProvider,
};
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::default()
                .add_directive("stream_download_audio=info".parse()?)
                .add_directive(LevelFilter::INFO.into()),
        )
        .with_line_number(true)
        .with_file(true)
        .init();

    // Progressive HTTP audio (MP3/AAC/FLAC etc.)
    let url = "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3".parse()?;

    // Decoder/buffering options.
    //
    // `pcm_chunk_frames` controls how many *sample-frames* we batch per `AudioMsg::Pcm`.
    // Too small => lots of wakeups/overhead on the rodio bridge.
    // Too large => bursty delivery and higher latency.
    //
    // A moderate value tends to work best with rodio for network streams.
    let opts = AudioDecodeOptions::default();

    // In-band control tap (HTTP typically doesn't emit controls, but the API is uniform).
    let (ctrl_tx, ctrl_rx) = mpsc::channel(128);

    // Build StreamDownload yourself so you can configure storage/settings precisely.
    let storage = TapStorageProvider::new(TempStorageProvider::default(), ctrl_tx);

    println!("Creating StreamDownload (HTTP)...");
    let reader = StreamDownload::new::<HttpStream<stream_download::http::reqwest::Client>>(
        url,
        storage,
        Settings::default(),
    )
    .await?;

    println!("Creating AudioDecodeStream (HTTP)...");
    let stream = AudioDecodeStream::new_from_stream_download(reader, ctrl_rx, opts).await?;
    println!("AudioDecodeStream created.");

    // Setup rodio output
    let stream_handle =
        OutputStreamBuilder::open_default_stream().expect("open default audio stream");
    let sink = Sink::connect_new(&stream_handle.mixer());

    // Adapt AudioDecodeStream into a rodio Source and play.
    let source = RodioSourceAdapter::new(stream);
    sink.append(source);
    sink.play();
    sink.sleep_until_end();

    Ok(())
}
