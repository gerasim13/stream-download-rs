//! Integration tests for HLS caching architecture
//!
//! These tests verify that the new HLS caching architecture works correctly
//! with stream-download-storage-ext components.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use stream_download::storage::StorageProvider;
use stream_download_hls::{
    CacheKeyGenerator, HlsCacheKey, HlsStorageProvider, SegmentedStorageProvider, VariantId,
    create_hls_cache_tree, create_hls_factory, create_hls_tree_factory, create_key_callback,
};
use tempfile::tempdir;
use url::Url;

#[test]
fn test_hls_cache_tree_integration() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let master_hash = "test_master_hash";

    // Create a cache tree using the factory function
    let cache_tree = create_hls_cache_tree(temp_dir.path(), master_hash)?;

    // Test with different types of HLS keys
    let playlist_key = HlsCacheKey::new(format!("{}/master.m3u8", master_hash));
    let variant_key = HlsCacheKey::new(format!("{}/0/variant.m3u8", master_hash));
    let segment_key = HlsCacheKey::new(format!("{}/0/seg_segment.ts", master_hash));
    let key_key = HlsCacheKey::new(format!("{}/0/key.bin", master_hash));

    // Test putting and getting data
    let test_data = Bytes::from_static(b"test data");

    cache_tree.put(&playlist_key, test_data.clone())?;
    assert!(cache_tree.exists(&playlist_key)?);

    let retrieved = cache_tree.get(&playlist_key)?.unwrap();
    assert_eq!(retrieved, test_data);

    // Test with multiple keys
    let variant_data = Bytes::from_static(b"variant playlist");
    cache_tree.put(&variant_key, variant_data.clone())?;

    let segment_data = Bytes::from_static(b"segment data");
    cache_tree.put(&segment_key, segment_data.clone())?;

    // Verify all data is stored separately
    assert_eq!(cache_tree.get(&playlist_key)?.unwrap(), test_data);
    assert_eq!(cache_tree.get(&variant_key)?.unwrap(), variant_data);
    assert_eq!(cache_tree.get(&segment_key)?.unwrap(), segment_data);

    // Test lease operations
    assert!(cache_tree.lease_exists());
    assert!(cache_tree.occupied());

    Ok(())
}

#[test]
fn test_cache_key_generator_integration() -> io::Result<()> {
    let temp_dir = tempdir()?;

    // Create CacheKeyGenerator
    let master_url = Url::parse("https://example.com/master.m3u8").unwrap();
    let generator = CacheKeyGenerator::new(&master_url);
    let master_hash = generator.master_hash();

    // Create cache tree
    let cache_tree = create_hls_cache_tree(temp_dir.path(), master_hash)?;

    // Generate keys for different resource types
    let playlist_url = "https://example.com/master.m3u8?token=123";
    let playlist_key = generator.playlist_key_from_url(playlist_url).unwrap();

    let variant_id = VariantId(0);
    let key_url = "https://example.com/key.bin?token=456";
    let key_key = generator.key_key_from_url(variant_id, key_url).unwrap();

    // Store data using generated keys
    let playlist_data = Bytes::from_static(b"#EXTM3U\n#EXT-X-VERSION:3");
    cache_tree.put(&playlist_key, playlist_data.clone())?;

    let key_data = Bytes::from_static(b"encryption key");
    cache_tree.put(&key_key, key_data.clone())?;

    // Verify data can be retrieved
    assert!(cache_tree.exists(&playlist_key)?);
    assert!(cache_tree.exists(&key_key)?);

    let retrieved_playlist = cache_tree.get(&playlist_key)?.unwrap();
    let retrieved_key = cache_tree.get(&key_key)?.unwrap();

    assert_eq!(retrieved_playlist, playlist_data);
    assert_eq!(retrieved_key, key_data);

    Ok(())
}

#[test]
fn test_hls_storage_provider_integration() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let master_hash = "test_master";

    // Create HlsStorageProvider
    let provider = HlsStorageProvider::new(temp_dir.path(), master_hash)?;

    // Test caching operations
    let playlist_key = HlsCacheKey::new(format!("{}/master.m3u8", master_hash));
    let playlist_data = Bytes::from_static(b"#EXTM3U\n#EXT-X-VERSION:3");

    provider.put(&playlist_key, playlist_data.clone())?;

    assert!(provider.exists(&playlist_key)?);
    assert_eq!(
        provider.len(&playlist_key)?,
        Some(playlist_data.len() as u64)
    );

    let retrieved = provider.get(&playlist_key)?.unwrap();
    assert_eq!(retrieved, playlist_data);

    // Test lease management
    assert!(!provider.lease_exists());

    provider.touch_lease()?;
    assert!(provider.lease_exists());
    assert!(provider.is_occupied());

    let age = provider.lease_age();
    assert!(age < std::time::Duration::from_secs(1));

    provider.maybe_evict()?; // Should not evict since lease is active
    assert!(provider.lease_exists());

    provider.remove_lease()?;
    assert!(!provider.lease_exists());

    Ok(())
}

#[test]
fn test_segmented_storage_provider_integration() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let master_hash = "test_master";

    // Create HlsStorageProvider (cache)
    let cache = HlsStorageProvider::new(temp_dir.path(), master_hash)?;

    // Create SegmentedStorageProvider
    let segmented_provider = SegmentedStorageProvider::new(cache);

    // Test caching different types of resources
    let playlist_key = segmented_provider.create_playlist_key(master_hash, "master.m3u8");
    let playlist_data = Bytes::from_static(b"#EXTM3U\n#EXT-X-VERSION:3");

    segmented_provider.cache_resource(&playlist_key, playlist_data.clone())?;
    assert!(segmented_provider.is_cached(&playlist_key)?);

    let retrieved_playlist = segmented_provider
        .get_cached_resource(&playlist_key)?
        .unwrap();
    assert_eq!(retrieved_playlist, playlist_data);

    // Test segment caching
    let segment_key = segmented_provider.create_segment_key(master_hash, 0, 1, "segment.ts");
    let segment_data = Bytes::from_static(b"segment data");

    segmented_provider.cache_resource(&segment_key, segment_data.clone())?;
    assert!(segmented_provider.is_cached(&segment_key)?);

    // Test key caching
    let key_key = segmented_provider.create_key_key(master_hash, 0, "key.bin");
    let key_data = Bytes::from_static(b"encryption key");

    segmented_provider.cache_resource(&key_key, key_data.clone())?;
    assert!(segmented_provider.is_cached(&key_key)?);

    Ok(())
}

#[test]
fn test_segmented_reader_writer_integration() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let master_hash = "test_master";

    // Create cache and segmented provider
    let cache = HlsStorageProvider::new(temp_dir.path(), master_hash)?;
    let segmented_provider = SegmentedStorageProvider::new(cache);

    // Get reader/writer pair
    let (mut reader, mut writer) = segmented_provider.into_reader_writer(None)?;

    // First, cache a segment
    let segment_key = segmented_provider.create_segment_key(master_hash, 0, 0, "segment0.ts");
    let segment_data = Bytes::from_static(b"This is segment 0 data for testing reading");

    segmented_provider.cache_resource(&segment_key, segment_data.clone())?;

    // Update internal state to point to this segment
    // Note: In real usage, this would be done by the HLS downloader
    {
        let cache_clone = segmented_provider.cache().clone();
        let mut state = segmented_provider.state.lock().unwrap();
        state
            .segments
            .insert(0, std::path::PathBuf::from(segment_key.as_str()));
    }

    // Test reading from cached segment
    let mut buffer = [0u8; 20];
    let bytes_read = reader.read(&mut buffer)?;

    assert_eq!(bytes_read, 20);
    assert_eq!(&buffer[..20], b"This is segment 0 da");

    // Test seeking and reading more
    reader.seek(io::SeekFrom::Start(10))?;

    let mut buffer2 = [0u8; 15];
    let bytes_read2 = reader.read(&mut buffer2)?;

    assert_eq!(bytes_read2, 15);
    assert_eq!(&buffer2[..15], b"segment 0 data");

    // Test writer - write a new segment
    writer.write_all(b"New segment data written by writer")?;

    // Finalize the segment
    let new_segment_key = segmented_provider.create_segment_key(master_hash, 0, 1, "segment1.ts");
    writer.finalize_current_segment(&new_segment_key)?;

    // Verify the new segment was cached
    assert!(segmented_provider.is_cached(&new_segment_key)?);

    let cached_new_segment = segmented_provider
        .get_cached_resource(&new_segment_key)?
        .unwrap();
    assert_eq!(
        cached_new_segment,
        Bytes::from_static(b"New segment data written by writer")
    );

    Ok(())
}

#[test]
fn test_factory_functions_integration() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let storage_root = temp_dir.path();

    // Test create_hls_factory
    let factory_fn = create_hls_factory(storage_root);

    let key = HlsCacheKey::new("master/variant/file".to_string());
    let cache = factory_fn(&key)?.expect("Should create cache");

    let test_data = Bytes::from_static(b"test");
    cache.put(test_data.clone())?;

    let retrieved = cache.get()?.unwrap();
    assert_eq!(retrieved, test_data);

    // Test create_hls_tree_factory
    let tree_factory = create_hls_tree_factory(storage_root);

    let node = tree_factory.node(&key)?.expect("Should create node");
    node.put(Bytes::from_static(b"node data"))?;

    let node_data = node.get()?.unwrap();
    assert_eq!(node_data, Bytes::from_static(b"node data"));

    Ok(())
}

#[test]
fn test_lease_management_integration() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let master_hash = "lease_test_master";

    // Create multiple cache trees for the same master
    let cache_tree1 = create_hls_cache_tree(temp_dir.path(), master_hash)?;
    let cache_tree2 = create_hls_cache_tree(temp_dir.path(), master_hash)?;

    // Both should see the same lease file
    assert!(cache_tree1.lease_exists());
    assert!(cache_tree2.lease_exists());

    // Touch lease from one
    cache_tree1.touch()?;

    // Both should reflect the updated lease
    let age1 = cache_tree1.age();
    let age2 = cache_tree2.age();

    // Ages should be very close (within 1ms tolerance for test)
    let diff = if age1 > age2 {
        age1 - age2
    } else {
        age2 - age1
    };
    assert!(diff < std::time::Duration::from_millis(1));

    // Test occupied state
    assert!(cache_tree1.occupied());
    assert!(cache_tree2.occupied());

    // Test maybe_evict when occupied (should not evict)
    cache_tree1.maybe_evict()?;
    assert!(cache_tree1.lease_exists());

    // Remove lease
    cache_tree1.remove()?;
    assert!(!cache_tree1.lease_exists());
    assert!(!cache_tree2.lease_exists());

    // Test maybe_evict when not occupied (should be no-op since already removed)
    cache_tree1.maybe_evict()?;
    assert!(!cache_tree1.lease_exists());

    Ok(())
}
