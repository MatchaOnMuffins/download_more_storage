use crate::checksum::sha256_hex;
use crate::cloud::CloudBackend;
use crate::error::Result;
use crate::util::{atomic_write, chunk_file_name, ensure_dir};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct LocalMockCloud {
    root: PathBuf,
}

impl LocalMockCloud {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn chunks_dir(&self) -> PathBuf {
        self.root.join("chunks")
    }

    pub fn chunk_path(&self, chunk_id: u64) -> PathBuf {
        self.chunks_dir().join(chunk_file_name(chunk_id))
    }

    pub fn init(&self) -> Result<()> {
        ensure_dir(&self.chunks_dir())
    }
}

impl CloudBackend for LocalMockCloud {
    fn fetch_chunk(&self, chunk_id: u64, expected_size: u64) -> Result<Option<Vec<u8>>> {
        let path = self.chunk_path(chunk_id);
        if !path.exists() {
            return Ok(None);
        }
        let mut data = fs::read(path)?;
        data.resize(expected_size as usize, 0);
        Ok(Some(data))
    }

    fn upload_chunk(&self, chunk_id: u64, _generation: u64, data: &[u8]) -> Result<String> {
        self.init()?;
        let path = self.chunk_path(chunk_id);
        atomic_write(&path, data)?;
        Ok(sha256_hex(data))
    }
}
