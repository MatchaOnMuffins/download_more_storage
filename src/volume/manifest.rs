use crate::error::{CloudError, Result};
use crate::util::{atomic_write, now_rfc3339_utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudManifest {
    pub format_version: u32,
    pub volume_id: String,
    pub volume_size_bytes: u64,
    pub chunk_size_bytes: u64,
    pub sector_size_bytes: u64,
    pub created_at: String,
    pub generation: u64,
    pub single_writer: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalManifest {
    pub format_version: u32,
    pub volume_id: String,
    pub cache_dir: PathBuf,
    pub cloud_provider: String,
    pub bucket: String,
    pub prefix: String,
    pub remote_dir: PathBuf,
    pub cache_max_bytes: u64,
    pub evict_low_watermark_bytes: u64,
}

#[derive(Debug, Serialize)]
struct ConfigToml<'a> {
    volume: ConfigVolume<'a>,
    cache: ConfigCache<'a>,
    cloud: ConfigCloud<'a>,
}

#[derive(Debug, Serialize)]
struct ConfigVolume<'a> {
    id: &'a str,
}

#[derive(Debug, Serialize)]
struct ConfigCache<'a> {
    dir: &'a Path,
    max_bytes: u64,
    evict_low_watermark_bytes: u64,
}

#[derive(Debug, Serialize)]
struct ConfigCloud<'a> {
    provider: &'a str,
    bucket: &'a str,
    prefix: &'a str,
    remote_dir: &'a Path,
}

impl CloudManifest {
    pub fn new(volume_id: String, volume_size_bytes: u64, chunk_size_bytes: u64) -> Self {
        Self {
            format_version: 1,
            volume_id,
            volume_size_bytes,
            chunk_size_bytes,
            sector_size_bytes: 4096,
            created_at: now_rfc3339_utc(),
            generation: 1,
            single_writer: true,
        }
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_vec_pretty(self)
            .map_err(|err| CloudError::Corrupt(format!("serialize cloud manifest: {err}")))?;
        atomic_write(path, &json)
    }

    pub fn read(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)?;
        serde_json::from_str(&text)
            .map_err(|err| CloudError::Corrupt(format!("parse cloud manifest: {err}")))
    }
}

impl LocalManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        volume_id: String,
        cache_dir: PathBuf,
        bucket: String,
        prefix: String,
        remote_dir: PathBuf,
        cache_max_bytes: u64,
    ) -> Self {
        let evict_low_watermark_bytes = cache_max_bytes.saturating_mul(9) / 10;
        Self {
            format_version: 1,
            volume_id,
            cache_dir,
            cloud_provider: "local_mock".to_string(),
            bucket,
            prefix,
            remote_dir,
            cache_max_bytes,
            evict_low_watermark_bytes,
        }
    }

    pub fn path(cache_dir: &Path) -> PathBuf {
        cache_dir.join("manifest.local.json")
    }

    pub fn write(&self) -> Result<()> {
        let json = serde_json::to_vec_pretty(self)
            .map_err(|err| CloudError::Corrupt(format!("serialize local manifest: {err}")))?;
        atomic_write(&Self::path(&self.cache_dir), &json)?;
        atomic_write(
            &self.cache_dir.join("cloudcache.toml"),
            self.to_toml()?.as_bytes(),
        )?;
        Ok(())
    }

    pub fn read(cache_dir: &Path) -> Result<Self> {
        let text = fs::read_to_string(Self::path(cache_dir))?;
        serde_json::from_str(&text)
            .map_err(|err| CloudError::Corrupt(format!("parse local manifest: {err}")))
    }

    fn to_toml(&self) -> Result<String> {
        let config = ConfigToml {
            volume: ConfigVolume {
                id: &self.volume_id,
            },
            cache: ConfigCache {
                dir: &self.cache_dir,
                max_bytes: self.cache_max_bytes,
                evict_low_watermark_bytes: self.evict_low_watermark_bytes,
            },
            cloud: ConfigCloud {
                provider: &self.cloud_provider,
                bucket: &self.bucket,
                prefix: &self.prefix,
                remote_dir: &self.remote_dir,
            },
        };
        toml::to_string_pretty(&config)
            .map_err(|err| CloudError::Corrupt(format!("serialize config TOML: {err}")))
    }
}
