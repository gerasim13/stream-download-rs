//! Playlist parser module.
//!
//! Adapter around the `hls_m3u8` crate that converts parsed playlists
//! into our internal `model` types.

use hls_m3u8::Decryptable;
use hls_m3u8::MasterPlaylist as HlsMasterPlaylist;
use hls_m3u8::MediaPlaylist as HlsMediaPlaylist;
use hls_m3u8::tags::VariantStream as HlsVariantStreamTag;

use crate::model::{
    CodecInfo, EncryptionMethod, HlsError, HlsResult, InitSegment, KeyInfo, MasterPlaylist,
    MediaPlaylist, MediaSegment, SegmentKey, VariantId, VariantStream,
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
            let (uri, bandwidth, codecs_str) = match vs {
                HlsVariantStreamTag::ExtXStreamInf {
                    uri, stream_data, ..
                } => {
                    let bw = stream_data.bandwidth();
                    let codecs = stream_data.codecs().map(|c| c.to_string());
                    (uri.to_string(), Some(bw), codecs)
                }
                HlsVariantStreamTag::ExtXIFrame { uri, stream_data } => {
                    let bw = stream_data.bandwidth();
                    let codecs = stream_data.codecs().map(|c| c.to_string());
                    (uri.to_string(), Some(bw), codecs)
                }
            };

            let codec = codecs_str.map(|c| CodecInfo {
                codecs: Some(c),
                audio_codec: None,
                container: None,
            });

            VariantStream {
                id: VariantId(index),
                uri,
                bandwidth,
                name: None,
                codec,
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
    // Derive end-of-stream strictly from presence of the EXT-X-ENDLIST tag.
    // Some servers set Playlist-Type=VOD or EVENT without a terminal ENDLIST.
    let end_list = input.contains("#EXT-X-ENDLIST");

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
                duration: seg.duration.duration(),
                key: seg_key,
            }
        })
        .collect();

    let init_segment = hls_media.segments.iter().next().and_then(|(_, seg)| {
        seg.map.as_ref().map(|m| {
            let map_key = m.keys().get(0).map(|k| {
                let method = EncryptionMethod::Aes128;
                let key_info = KeyInfo {
                    method: method.clone(),
                    uri: Some(k.uri().to_string()),
                    iv: k.iv.to_slice(),
                    key_format: k.format.as_ref().map(|s| s.to_string()),
                    key_format_versions: k.versions.as_ref().map(|s| s.to_string()),
                };
                SegmentKey {
                    method,
                    key_info: Some(key_info),
                }
            });
            InitSegment {
                uri: m.uri().to_string(),
                key: map_key,
            }
        })
    });

    Ok(MediaPlaylist {
        segments,
        target_duration,
        init_segment,
        media_sequence,
        end_list,
        current_key,
    })
}
