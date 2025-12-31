use std::fmt::Debug;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use super::file::FileStorageProvider;
use stream_download::storage::StorageProvider;
use tracing::error;

/// Lease trait for managing leases on storage providers.
pub trait Lease {
    fn touch(&self) -> io::Result<()>;
    fn remove(&self) -> io::Result<()>;
    fn exists(&self) -> bool;
    fn age(&self) -> Duration;
}

/// LeaseAwareStorageProvider trait for managing leases on storage providers.
pub trait LeaseAwareStorageProvider: StorageProvider + Lease {
    const LEASE_TTL: Duration = Duration::from_secs(6 * 60 * 60);
    /// Check if the storage provider is occupied.
    fn occupied(&self) -> bool {
        self.exists() && self.age() < Self::LEASE_TTL
    }
    /// Maybe evict the storage provider.
    fn maybe_evict(&self) -> io::Result<()> {
        if !self.occupied() {
            self.remove()
        } else {
            Ok(())
        }
    }
}

/// LeaseFile struct for managing leases on storage providers.
#[derive(Debug)]
pub struct LeaseFile {
    path: PathBuf,
}

impl LeaseFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

fn get_file_age(path: &PathBuf) -> Duration {
    match fs::metadata(path) {
        Ok(metadata) => {
            let modified = metadata.modified().unwrap();
            match SystemTime::now().duration_since(modified) {
                Ok(age) => age,
                Err(_) => Duration::ZERO,
            }
        }
        Err(_) => Duration::ZERO,
    }
}

impl Lease for LeaseFile {
    fn touch(&self) -> io::Result<()> {
        let file = OpenOptions::new().write(true).open(&self.path)?;
        file.set_len(0)
    }

    fn remove(&self) -> io::Result<()> {
        if self.exists() {
            fs::remove_file(&self.path)
        } else {
            Ok(())
        }
    }

    fn exists(&self) -> bool {
        self.path.exists()
    }

    fn age(&self) -> Duration {
        get_file_age(&self.path)
    }
}

impl Drop for LeaseFile {
    fn drop(&mut self) {
        if let Err(e) = self.remove() {
            error!("Failed to remove lease file: {}", e);
        }
    }
}

// #[derive(Debug)]
// pub struct LeaseAwareFileStorageProvider<P, L>
// where
//     P: StorageProvider + Debug,
//     L: Lease + Debug,
// {
//     inner: P,
//     lease: L,
// }

// impl LeaseAwareFileStorageProvider<FileStorageProvider, LeaseFile> {
//     pub fn new(inner: FileStorageProvider, lease: LeaseFile) -> Self {
//         Self { inner, lease }
//     }
// }

// impl Lease for LeaseAwareFileStorageProvider<FileStorageProvider, LeaseFile> {
//     fn touch(&self) -> io::Result<()> {
//         self.lease.touch()
//     }

//     fn remove(&self) -> io::Result<()> {
//         if self.exists() {
//             fs::remove_file(&self.inner.path())
//         } else {
//             Ok(())
//         }
//     }

//     fn exists(&self) -> bool {
//         self.inner.path().exists()
//     }

//     fn age(&self) -> Duration {
//         get_file_age(&self.inner.path())
//     }
// }
