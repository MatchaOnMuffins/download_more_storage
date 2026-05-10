use crate::checksum::sha256_hex;
use crate::cloud::CloudBackend;
use crate::error::{CloudError, Result};
use crate::util::{atomic_write, chunk_file_name, ensure_dir};
use crate::volume::manifest::CloudManifest;
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

    fn ensure_layout(&self) -> Result<()> {
        ensure_dir(&self.chunks_dir())
    }
}

impl CloudBackend for LocalMockCloud {
    fn init(&self, manifest: &CloudManifest) -> Result<()> {
        self.ensure_layout()?;
        manifest.write(&self.root.join("manifest.json"))
    }

    fn describe(&self) -> String {
        format!("local_mock:{}", self.root.display())
    }

    fn fetch_chunk(&self, chunk_id: u64, expected_size: u64) -> Result<Option<Vec<u8>>> {
        let path = self.chunk_path(chunk_id);
        if !path.exists() {
            return Ok(None);
        }
        let data = fs::read(path)?;
        if data.len() as u64 != expected_size {
            return Err(CloudError::Corrupt(format!(
                "remote chunk {chunk_id} size {} does not match expected size {expected_size}",
                data.len()
            )));
        }
        Ok(Some(data))
    }

    fn upload_chunk(&self, chunk_id: u64, _generation: u64, data: &[u8]) -> Result<String> {
        self.ensure_layout()?;
        let path = self.chunk_path(chunk_id);
        atomic_write(&path, data)?;
        Ok(sha256_hex(data))
    }
}
