//! Test fixtures for stream-download-audio integration tests.
//!
//! Unlike HlsFixture which generates fake bytes for testing HLS protocol layer,
//! AudioFixture provides REAL audio data for testing the audio decoding pipeline.
//!
//! Strategy:
//! - HTTP tests: Generate WAV files on-the-fly (simple format, no external dependencies)
//! - HLS tests: Use real fMP4 segments (future: include_bytes! or generate via ffmpeg)
//! - Unit tests: Mock PacketProducer for testing infrastructure without decoder

use std::sync::Arc;
use axum::{Router, routing::get, response::IntoResponse, http::StatusCode};
use bytes::Bytes;

/// Helper for testing audio decoding and pipeline.
pub struct AudioFixture;

/// Simple HTTP server that serves a single audio file.
/// 
/// This is used for testing AudioStream with real audio data.
pub struct SimpleAudioServer {
    audio_data: Arc<Bytes>,
}

impl SimpleAudioServer {
    /// Create a new server that will serve the given audio data.
    pub fn new(audio_data: Vec<u8>) -> Self {
        Self {
            audio_data: Arc::new(Bytes::from(audio_data)),
        }
    }

    /// Create an axum Router that serves the audio file at /audio.wav
    pub fn router(&self) -> Router {
        let audio_data = Arc::clone(&self.audio_data);
        
        Router::new().route(
            "/audio.wav",
            get(move || {
                let data = Arc::clone(&audio_data);
                async move {
                    (StatusCode::OK, data.as_ref().clone())
                }
            }),
        )
    }
}

impl AudioFixture {
    /// Generate a simple WAV file containing a sine wave.
    ///
    /// This is used for HTTP integration tests because WAV is the simplest audio format
    /// to generate without external dependencies. Symphonia can decode it.
    ///
    /// # Arguments
    /// * `freq_hz` - Frequency of the sine wave (e.g. 440.0 for A4)
    /// * `duration_secs` - Duration in seconds
    /// * `sample_rate` - Sample rate (e.g. 44100 or 48000)
    /// * `channels` - Number of channels (1=mono, 2=stereo)
    ///
    /// # Returns
    /// Complete WAV file as bytes (header + PCM samples)
    pub fn generate_sine_wav(
        freq_hz: f32,
        duration_secs: f32,
        sample_rate: u32,
        channels: u16,
    ) -> Vec<u8> {
        let num_samples = (sample_rate as f32 * duration_secs) as usize;
        let mut wav = Vec::new();

        // WAV header (44 bytes)
        wav.extend_from_slice(b"RIFF");
        let file_size = 36 + (num_samples * channels as usize * 2); // 16-bit samples
        wav.extend_from_slice(&(file_size as u32).to_le_bytes());
        wav.extend_from_slice(b"WAVE");

        // fmt chunk
        wav.extend_from_slice(b"fmt ");
        wav.extend_from_slice(&16u32.to_le_bytes()); // fmt chunk size
        wav.extend_from_slice(&1u16.to_le_bytes()); // PCM format
        wav.extend_from_slice(&channels.to_le_bytes());
        wav.extend_from_slice(&sample_rate.to_le_bytes());
        let byte_rate = sample_rate * channels as u32 * 2; // 16-bit = 2 bytes
        wav.extend_from_slice(&byte_rate.to_le_bytes());
        let block_align = channels * 2;
        wav.extend_from_slice(&block_align.to_le_bytes());
        wav.extend_from_slice(&16u16.to_le_bytes()); // bits per sample

        // data chunk
        wav.extend_from_slice(b"data");
        let data_size = num_samples * channels as usize * 2;
        wav.extend_from_slice(&(data_size as u32).to_le_bytes());

        // Generate PCM samples (16-bit signed integers)
        for i in 0..num_samples {
            let t = i as f32 / sample_rate as f32;
            let sample_f32 = (2.0 * std::f32::consts::PI * freq_hz * t).sin();
            let sample_i16 = (sample_f32 * i16::MAX as f32) as i16;

            // Write the same sample to all channels
            for _ in 0..channels {
                wav.extend_from_slice(&sample_i16.to_le_bytes());
            }
        }

        wav
    }

    /// Verify that PCM samples are not silent.
    ///
    /// This is a basic sanity check - at least 10% of samples should be above threshold.
    pub fn verify_samples_not_silence(samples: &[f32]) -> bool {
        if samples.is_empty() {
            return false;
        }

        let threshold = 0.01;
        let non_silent = samples.iter().filter(|&&s| s.abs() > threshold).count();
        
        // Require at least 10% of samples to be above threshold
        non_silent > samples.len() / 10
    }

    /// Verify that decoded PCM has expected characteristics.
    ///
    /// Checks that the number of samples is within 10% tolerance of expected
    /// (accounting for buffering, rounding, etc.)
    pub fn verify_sample_count(
        samples: &[f32],
        expected_sample_rate: u32,
        expected_channels: u16,
        duration_hint: f32,
    ) -> bool {
        let expected_total_samples =
            (expected_sample_rate as f32 * duration_hint * expected_channels as f32) as usize;

        // Allow 10% tolerance
        let min_samples = (expected_total_samples as f32 * 0.9) as usize;
        let max_samples = (expected_total_samples as f32 * 1.1) as usize;

        samples.len() >= min_samples && samples.len() <= max_samples
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_sine_wav() {
        let wav = AudioFixture::generate_sine_wav(440.0, 0.1, 44100, 2);

        // Check WAV header
        assert_eq!(&wav[0..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");
        assert_eq!(&wav[12..16], b"fmt ");

        // Check that we have data
        assert!(wav.len() > 44); // Header + samples
    }

    #[test]
    fn test_verify_samples_not_silence() {
        let silent = vec![0.0; 1000];
        assert!(!AudioFixture::verify_samples_not_silence(&silent));

        let mut noisy = vec![0.0; 1000];
        for i in 0..200 {
            noisy[i] = 0.5; // 20% non-silent
        }
        assert!(AudioFixture::verify_samples_not_silence(&noisy));
    }
}
