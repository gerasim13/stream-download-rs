use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter};
use std::path::{Path, PathBuf};

use stream_download::storage::StorageProvider;

/// Storage provider that persists bytes into a single file on disk.
#[derive(Clone, Debug)]
pub struct FileStorageProvider {
    path: PathBuf,
}

impl FileStorageProvider {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        // Ensure parent exists (common for cache tree layouts).
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("error creating storage parent directory: {}", e),
                )
            })?;
        }
        Ok(Self { path })
    }

    /// Return the underlying file path.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Open the file for read/write access.
    fn open_for_rw(&self) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.path)
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("error opening storage file: {}", e),
                )
            })
    }
}

impl StorageProvider for FileStorageProvider {
    type Reader = BufReader<File>;
    type Writer = BufWriter<File>;

    fn into_reader_writer(
        self,
        _content_length: Option<u64>,
    ) -> io::Result<(Self::Reader, Self::Writer)> {
        let reader_file = self.open_for_rw()?;
        let writer_file = reader_file.try_clone().map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("error cloning file for writer: {}", e),
            )
        })?;

        let reader = BufReader::new(reader_file);
        let writer = BufWriter::new(writer_file);

        Ok((reader, writer))
    }
}
