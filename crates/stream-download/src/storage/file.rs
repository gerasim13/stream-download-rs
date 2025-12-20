//! Storage implementation backed by a filesystem file handle.
//!
//! This provider is intended for **persistent** storage where each provider instance maps to
//! exactly one file on disk (a single resource).
//!
//! Design goals:
//! - **Lock-free hot path**: no shared `Mutex`/`RwLock` between reader/writer.
//! - Reader and writer each use their own `File` handle, created via `try_clone()`.
//! - Correct `Read + Seek` / `Write + Seek` behavior for the storage layer.
//!
//! Notes:
//! - Concurrent read/write visibility is OS/filesystem dependent. In practice on Unix/macOS,
//!   a reader can observe newly-written bytes once the writer flushes its buffers.
//! - This module intentionally does not provide any "read whole resource" handle API to avoid
//!   contention. Segment caching should be implemented via deterministic file layout and normal
//!   `Read + Seek` readers.

use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};

use super::{ContentLength, StorageProvider, StorageWriter};
use crate::WrapIoResult;

/// Storage provider that persists bytes into a single file on disk.
///
/// This is a single-resource provider: one provider instance corresponds to one file path.
#[derive(Clone, Debug)]
pub struct FileStorageProvider {
    path: PathBuf,
}

impl FileStorageProvider {
    /// Open (or create) the file at `path` for reading and writing.
    ///
    /// The file is created if it does not exist.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Ensure parent exists (common for cache tree layouts).
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).wrap_err("error creating storage parent directory")?;
        }

        // Touch the file so subsequent opens/try_clone succeed consistently.
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .wrap_err("error opening storage file")?;

        Ok(Self { path })
    }

    /// Return the underlying file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn open_for_rw(&self) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.path)
            .wrap_err("error opening storage file")
    }
}

impl StorageProvider for FileStorageProvider {
    type Reader = FileStorageReader;
    type Writer = FileStorageWriter;

    fn into_reader_writer(
        self,
        _content_length: ContentLength,
    ) -> io::Result<(Self::Reader, Self::Writer)> {
        // Open one handle, then clone to ensure both point to the same inode/path.
        let base = self.open_for_rw()?;

        let reader_file = base.try_clone().wrap_err("error cloning file for reader")?;
        let writer_file = base;

        let reader = FileStorageReader {
            reader: BufReader::new(reader_file),
        };
        let writer = FileStorageWriter {
            writer: BufWriter::new(writer_file),
        };

        Ok((reader, writer))
    }
}

/// Reader created by a [`FileStorageProvider`]. Reads from the persistent file.
#[derive(Debug)]
pub struct FileStorageReader {
    reader: BufReader<File>,
}

impl Read for FileStorageReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

impl Seek for FileStorageReader {
    fn seek(&mut self, position: io::SeekFrom) -> io::Result<u64> {
        self.reader.seek(position)
    }
}

/// Writer created by a [`FileStorageProvider`]. Writes to the persistent file.
#[derive(Debug)]
pub struct FileStorageWriter {
    writer: BufWriter<File>,
}

impl StorageWriter for FileStorageWriter {}

impl Write for FileStorageWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl Seek for FileStorageWriter {
    fn seek(&mut self, position: io::SeekFrom) -> io::Result<u64> {
        self.writer.seek(position)
    }
}
