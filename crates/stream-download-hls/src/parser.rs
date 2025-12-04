//! Playlist parser module.
//!
//! Adapter around the `hls_m3u8` crate that converts parsed playlists
//! into our internal `model` types.

use hls_m3u8::Decryptable;
use hls_m3u8::MasterPlaylist as HlsMasterPlaylist;
use hls_m3u8::MediaPlaylist as HlsMediaPlaylist;
use hls_m3u8::tags::VariantStream as HlsVariantStreamTag;
use hls_m3u8::types::PlaylistType;
use std::time::Duration;

use crate::model::{
    EncryptionMethod, HlsError, HlsResult, KeyInfo, MasterPlaylist, MediaPlaylist, MediaSegment,
    SegmentKey, VariantId, VariantStream,
};

/// Parse a master playlist (M3U8) into a [`MasterPlaylist`].
pub fn parse_master_playlist(data: &[u8]) -> HlsResult<MasterPlaylist> {
    let input = std::str::from_utf8(data)
        .map_err(|e| HlsError::InvalidPlaylist(format!("invalid UTF-8: {}", e)))?;
    let hls_master = HlsMasterPlaylist::try_from(input)
        .map_err(|e| HlsError::InvalidPlaylist(format!("hls_m3u8 parse error: {}", e)))?
        .into_owned();

    let variants = hls_master
        .variant_streams
        .iter()
        .enumerate()
        .map(|(index, vs)| {
            let uri = match vs {
                HlsVariantStreamTag::ExtXStreamInf { uri, .. } => uri.to_string(),
                HlsVariantStreamTag::ExtXIFrame { uri, .. } => uri.to_string(),
            };

            VariantStream {
                id: VariantId(index),
                uri,
                bandwidth: None,
                name: None,
                codec: None,
            }
        })
        .collect();

    Ok(MasterPlaylist { variants })
}

/// Parse a media playlist (M3U8) into a [`MediaPlaylist`].
pub fn parse_media_playlist(data: &[u8], variant_id: VariantId) -> HlsResult<MediaPlaylist> {
    let input = std::str::from_utf8(data)
        .map_err(|e| HlsError::InvalidPlaylist(format!("invalid UTF-8: {}", e)))?;
    let hls_media = HlsMediaPlaylist::try_from(input)
        .map_err(|e| HlsError::InvalidPlaylist(format!("hls_m3u8 parse error: {}", e)))?
        .into_owned();

    let target_duration = Some(hls_media.target_duration);
    let media_sequence = hls_media.media_sequence as u64;
    let end_list = matches!(hls_media.playlist_type, Some(PlaylistType::Vod));

    // TODO: handle EXT-X-KEY and SAMPLE-AES via Decryptable if/when needed.
    let current_key: Option<KeyInfo> = None;

    let segments = hls_media
        .segments
        .iter()
        .enumerate()
        .map(|(index, (_idx, seg))| {
            // Basic key/IV extraction using Decryptable
            let seg_key = seg.keys().get(0).map(|k| {
                let method = EncryptionMethod::Aes128; // minimal assumption for now
                let key_info = KeyInfo {
                    method: method.clone(),
                    uri: Some(k.uri().to_string()),
                    iv: k.iv.to_slice(), // Option<[u8;16]>
                    key_format: k.format.as_ref().map(|s| s.to_string()),
                    key_format_versions: k.versions.as_ref().map(|s| s.to_string()),
                };
                SegmentKey {
                    method,
                    key_info: Some(key_info),
                }
            });

            MediaSegment {
                sequence: media_sequence + index as u64,
                variant_id,
                uri: seg.uri().to_string(),
                duration: Duration::from_secs(0),
                key: seg_key,
            }
        })
        .collect();

    Ok(MediaPlaylist {
        segments,
        target_duration,
        media_sequence,
        end_list,
        current_key,
    })
}
