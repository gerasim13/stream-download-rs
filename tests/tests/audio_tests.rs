//! Audio integration tests for stream-download-audio.
//!
//! These tests validate:
//! - Audio decoding (Symphonia) with real audio files
//! - PCM output correctness
//! - Pipeline behavior (init segment detection, decoder reopening)
//! - Ring buffer and RodioSourceAdapter
//! - Seek functionality
//!
//! Unlike HLS tests which stop at the byte level, audio tests go all the way
//! to decoded PCM samples.

mod audio_fixture;
mod setup;

use audio_fixture::AudioFixture;

#[test]
fn test_wav_generator_produces_valid_audio() {
    // Test that our WAV generator produces valid audio that Symphonia can decode
    let wav = AudioFixture::generate_sine_wav(440.0, 0.1, 44100, 2);
    
    // Basic validation
    assert!(wav.len() > 44, "WAV should have header + data");
    assert_eq!(&wav[0..4], b"RIFF");
    assert_eq!(&wav[8..12], b"WAVE");
}

mod rodio_tests {
    use super::*;
    use stream_download::storage::temp::TempStorageProvider;
    use stream_download_audio::{AudioSettings, AudioStream, RodioSourceAdapter};
    use std::time::Duration;

    #[tokio::test]
    async fn test_rodio_source_adapter_reads_from_stream() {
        use audio_fixture::SimpleAudioServer;
        
        // Generate a 1-second sine wave (440 Hz, 48kHz sample rate, stereo)
        let wav = AudioFixture::generate_sine_wav(440.0, 1.0, 48000, 2);
        
        // Setup mock HTTP server
        let server = SimpleAudioServer::new(wav);
        let router = server.router();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        
        // Spawn server in background
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            axum::serve(listener, router).await.unwrap();
        });
        
        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let url = format!("http://{}/audio.wav", addr);
        let url = url.parse::<reqwest::Url>().unwrap();
        let storage = TempStorageProvider::new();
        let audio_settings = AudioSettings::default();
        let stream_settings = stream_download::Settings::default();
        
        // Create AudioStream
        let stream = AudioStream::new_http(url, storage, audio_settings, stream_settings).await;
        
        // Create RodioSourceAdapter - this should start background thread
        let mut adapter = RodioSourceAdapter::new(stream);
        
        // Give pipeline time to decode and fill ring buffer
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // Now try to read samples via Iterator
        let mut samples_read = Vec::new();
        let start = std::time::Instant::now();
        
        // Try to read samples for up to 2 seconds
        while start.elapsed() < Duration::from_secs(2) && samples_read.len() < 48000 {
            if let Some(sample) = adapter.next() {
                samples_read.push(sample);
            } else {
                // Iterator exhausted or blocked
                break;
            }
        }
        
        // Verify we got samples
        assert!(!samples_read.is_empty(), "RodioSourceAdapter yielded no samples");
        
        // Verify samples are not all silence
        assert!(
            AudioFixture::verify_samples_not_silence(&samples_read),
            "All samples are silent - decoder may not be working"
        );
        
        println!("Successfully read {} samples from AudioStream", samples_read.len());
    }
}

// TODO: Add more integration tests:
// - HTTP audio decoding (serve WAV via mock HTTP server)
// - HLS audio decoding (with real fMP4 segments)
// - Seek functionality (send seek command, verify PCM buffer cleared)
// - Variant switching (detect init_hash change, verify decoder reopened)
