use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use stream_download::storage::{StorageProvider, StorageReader, StorageWriter};

/// Trait for caching a single blob of data.
pub trait BlobCache: StorageProvider {
    /// Get the cached blob, if present.
    fn get(&self) -> io::Result<Option<Bytes>>;
    /// Store a blob in the cache, overwriting any existing data.
    fn put(&self, data: Bytes) -> io::Result<()>;
    /// Get the length of the cached blob, if present.
    fn len(&self) -> io::Result<Option<u64>>;
    /// Check if the cache contains a blob.
    fn exists(&self) -> io::Result<bool>;
}

/// Trait for reading a cached blob.
pub trait BlobCacheReader: StorageReader {
    /// Get the cached blob, if present.
    fn get(&self) -> io::Result<Option<Bytes>>;
    /// Get the length of the cached blob, if present.
    fn len(&self) -> io::Result<Option<u64>>;
    /// Check if the cache contains a blob.
    fn exists(&self) -> io::Result<bool>;
}

/// Trait for writing a cached blob.
pub trait BlobCacheWriter: StorageWriter {
    /// Store a blob in the cache, overwriting any existing data.
    fn put(&self, data: Bytes) -> io::Result<()>;
}

/// Storage-backed implementation of `BlobCacheReader`.
#[derive(Default, Clone, Debug)]
pub struct StorageBackedBlobCacheReader<R>
where
    R: StorageReader,
{
    inner: Arc<Mutex<R>>,
    actual_len: Arc<AtomicU64>,
}

impl<R> StorageBackedBlobCacheReader<R>
where
    R: StorageReader,
{
    pub fn new(reader: R, actual_len: Arc<AtomicU64>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(reader)),
            actual_len,
        }
    }
}

impl<R> BlobCacheReader for StorageBackedBlobCacheReader<R>
where
    R: StorageReader,
{
    fn get(&self) -> io::Result<Option<Bytes>> {
        let actual_len = self.actual_len.load(Ordering::SeqCst);

        // Empty cache is treated as a miss
        if actual_len > 0
            && let Ok(mut reader) = self.inner.lock()
        {
            // Store current position
            let current_pos = reader.seek(SeekFrom::Current(0))?;
            // Read the actual content length
            reader.seek(SeekFrom::Start(0))?;
            let mut vec_buffer = vec![0u8; actual_len as usize];
            reader.read_exact(&mut vec_buffer)?;
            // Restore original position
            reader.seek(SeekFrom::Start(current_pos))?;
            return Ok(Some(Bytes::from(vec_buffer)));
        }

        return Ok(None);
    }

    fn len(&self) -> io::Result<Option<u64>> {
        let len = self.actual_len.load(Ordering::SeqCst);
        if len == 0 { Ok(None) } else { Ok(Some(len)) }
    }

    fn exists(&self) -> io::Result<bool> {
        let len = self.actual_len.load(Ordering::SeqCst);
        Ok(len > 0)
    }
}

impl<R> Read for StorageBackedBlobCacheReader<R>
where
    R: StorageReader,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if let Ok(mut reader) = self.inner.lock() {
            reader.read(buf)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock",
            ))
        }
    }
}

impl<R> Seek for StorageBackedBlobCacheReader<R>
where
    R: StorageReader,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if let Ok(mut reader) = self.inner.lock() {
            reader.seek(pos)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock",
            ))
        }
    }
}

/// Storage-backed implementation of `BlobCacheWriter`.
#[derive(Default, Clone, Debug)]
pub struct StorageBackedBlobCacheWriter<W>
where
    W: StorageWriter,
{
    inner: Arc<Mutex<W>>,
    actual_len: Arc<AtomicU64>,
}

impl<W> StorageBackedBlobCacheWriter<W>
where
    W: StorageWriter,
{
    pub fn new(writer: W, actual_len: Arc<AtomicU64>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(writer)),
            actual_len,
        }
    }
}

impl<W> BlobCacheWriter for StorageBackedBlobCacheWriter<W>
where
    W: StorageWriter,
{
    fn put(&self, data: Bytes) -> io::Result<()> {
        if let Ok(mut writer) = self.inner.lock() {
            // Overwrite the entire object
            writer.seek(SeekFrom::Start(0))?;
            writer.write_all(&data)?;
            writer.flush()?;
            // Update the actual data length
            self.actual_len.store(data.len() as u64, Ordering::SeqCst);
        }

        Ok(())
    }
}

impl<W> Write for StorageBackedBlobCacheWriter<W>
where
    W: StorageWriter,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Ok(mut writer) = self.inner.lock() {
            writer.write(buf)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock",
            ))
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Ok(mut writer) = self.inner.lock() {
            writer.flush()
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock",
            ))
        }
    }
}

impl<W> Seek for StorageBackedBlobCacheWriter<W>
where
    W: StorageWriter,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if let Ok(mut writer) = self.inner.lock() {
            writer.seek(pos)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock",
            ))
        }
    }
}

/// A `BlobCache` implementation backed by a `StorageProvider`.
#[derive(Default, Clone, Debug)]
pub struct StorageBackedBlobCache<P>
where
    P: StorageProvider,
{
    reader: StorageBackedBlobCacheReader<P::Reader>,
    writer: StorageBackedBlobCacheWriter<P::Writer>,
    _actual_len: Arc<AtomicU64>,
}

impl<P> StorageBackedBlobCache<P>
where
    P: StorageProvider,
{
    pub fn new(provider: P) -> io::Result<Self> {
        let actual_len = Arc::new(AtomicU64::new(0));
        let (reader, writer) = provider.into_reader_writer(None)?;
        Ok(Self {
            reader: StorageBackedBlobCacheReader::new(reader, actual_len.clone()),
            writer: StorageBackedBlobCacheWriter::new(writer, actual_len.clone()),
            _actual_len: actual_len,
        })
    }
}

impl<P> BlobCache for StorageBackedBlobCache<P>
where
    P: StorageProvider,
{
    fn get(&self) -> io::Result<Option<Bytes>> {
        self.reader.get()
    }

    fn put(&self, data: Bytes) -> io::Result<()> {
        self.writer.put(data)
    }

    fn len(&self) -> io::Result<Option<u64>> {
        self.reader.len()
    }

    fn exists(&self) -> io::Result<bool> {
        self.reader.exists()
    }
}

impl<P> StorageProvider for StorageBackedBlobCache<P>
where
    P: StorageProvider,
{
    type Reader = StorageBackedBlobCacheReader<P::Reader>;
    type Writer = StorageBackedBlobCacheWriter<P::Writer>;

    fn into_reader_writer(
        self,
        _content_length: Option<u64>,
    ) -> io::Result<(Self::Reader, Self::Writer)> {
        Ok((self.reader, self.writer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stream_download::storage::memory::MemoryStorageProvider;

    #[test]
    fn test_memory_cache_roundtrip() -> io::Result<()> {
        let provider = MemoryStorageProvider;
        let cache = StorageBackedBlobCache::new(provider)?;

        // Initially empty
        assert!(!cache.exists()?);
        assert_eq!(cache.len()?, None);
        assert_eq!(cache.get()?, None);

        // Put data
        let data = Bytes::from("hello world");
        cache.put(data.clone())?;

        // Verify data exists
        assert!(cache.exists()?);
        assert_eq!(cache.len()?, Some(data.len() as u64));
        assert_eq!(cache.get()?, Some(data));

        Ok(())
    }

    #[test]
    fn test_memory_cache_overwrite() -> io::Result<()> {
        let provider = MemoryStorageProvider;
        let cache = StorageBackedBlobCache::new(provider)?;

        // Put initial data
        let initial_data = Bytes::from("initial data");
        cache.put(initial_data.clone())?;
        assert_eq!(cache.get()?, Some(initial_data));

        // Overwrite with new data
        let new_data = Bytes::from("new data");
        cache.put(new_data.clone())?;

        // Verify new data
        assert_eq!(cache.len()?, Some(new_data.len() as u64));
        assert_eq!(cache.get()?, Some(new_data));

        Ok(())
    }

    #[test]
    fn test_memory_cache_empty() -> io::Result<()> {
        let provider = MemoryStorageProvider;
        let cache = StorageBackedBlobCache::new(provider)?;

        // Put empty data
        let empty_data = Bytes::from("");
        cache.put(empty_data)?;

        // Empty cache should be treated as miss
        assert!(!cache.exists()?);
        assert_eq!(cache.len()?, None);
        assert_eq!(cache.get()?, None);

        Ok(())
    }
}
