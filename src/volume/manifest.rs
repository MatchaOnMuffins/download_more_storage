use crate::error::{CloudError, Result};
use crate::util::{atomic_write, now_rfc3339_utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

pub const DEFAULT_SECTOR_SIZE_BYTES: u64 = 4096;
const SUPPORTED_FORMAT_VERSION: u32 = 1;

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
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub endpoint_url: Option<String>,
    pub cache_max_bytes: u64,
    pub evict_low_watermark_bytes: u64,
}

pub struct CloudConfig {
    pub provider: String,
    pub bucket: String,
    pub prefix: String,
    pub remote_dir: PathBuf,
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint_url: Option<&'a str>,
}

impl CloudManifest {
    pub fn new(volume_id: String, volume_size_bytes: u64, chunk_size_bytes: u64) -> Self {
        Self {
            format_version: SUPPORTED_FORMAT_VERSION,
            volume_id,
            volume_size_bytes,
            chunk_size_bytes,
            sector_size_bytes: DEFAULT_SECTOR_SIZE_BYTES,
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
        let manifest: Self = serde_json::from_str(&text)
            .map_err(|err| CloudError::Corrupt(format!("parse cloud manifest: {err}")))?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        if self.format_version != SUPPORTED_FORMAT_VERSION {
            return Err(CloudError::Unsupported(format!(
                "cloud manifest format version {}",
                self.format_version
            )));
        }
        if self.volume_id.is_empty() {
            return Err(CloudError::Corrupt(
                "cloud manifest has empty volume id".to_string(),
            ));
        }
        if self.volume_size_bytes == 0 || self.chunk_size_bytes == 0 {
            return Err(CloudError::Corrupt(
                "cloud manifest size fields must be non-zero".to_string(),
            ));
        }
        if self.sector_size_bytes == 0 {
            return Err(CloudError::Corrupt(
                "cloud manifest sector size must be non-zero".to_string(),
            ));
        }
        if !self
            .volume_size_bytes
            .is_multiple_of(self.sector_size_bytes)
        {
            return Err(CloudError::Corrupt(
                "cloud manifest volume size is not sector-aligned".to_string(),
            ));
        }
        if !self.chunk_size_bytes.is_multiple_of(self.sector_size_bytes) {
            return Err(CloudError::Corrupt(
                "cloud manifest chunk size is not sector-aligned".to_string(),
            ));
        }
        Ok(())
    }
}

impl LocalManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        volume_id: String,
        cache_dir: PathBuf,
        cloud: CloudConfig,
        cache_max_bytes: u64,
    ) -> Self {
        let evict_low_watermark_bytes = cache_max_bytes.saturating_mul(9) / 10;
        Self {
            format_version: SUPPORTED_FORMAT_VERSION,
            volume_id,
            cache_dir,
            cloud_provider: cloud.provider,
            bucket: cloud.bucket,
            prefix: cloud.prefix,
            remote_dir: cloud.remote_dir,
            region: cloud.region,
            endpoint_url: cloud.endpoint_url,
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
        let mut manifest: Self = serde_json::from_str(&text)
            .map_err(|err| CloudError::Corrupt(format!("parse local manifest: {err}")))?;
        manifest.validate()?;
        manifest.cache_dir = cache_dir.to_path_buf();
        Ok(manifest)
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
                region: self.region.as_deref(),
                endpoint_url: self.endpoint_url.as_deref(),
            },
        };
        toml::to_string_pretty(&config)
            .map_err(|err| CloudError::Corrupt(format!("serialize config TOML: {err}")))
    }

    pub fn validate(&self) -> Result<()> {
        if self.format_version != SUPPORTED_FORMAT_VERSION {
            return Err(CloudError::Unsupported(format!(
                "local manifest format version {}",
                self.format_version
            )));
        }
        if self.volume_id.is_empty() {
            return Err(CloudError::Corrupt(
                "local manifest has empty volume id".to_string(),
            ));
        }
        if !matches!(self.cloud_provider.as_str(), "local_mock" | "s3") {
            return Err(CloudError::Unsupported(format!(
                "cloud provider '{}'",
                self.cloud_provider
            )));
        }
        if self.bucket.is_empty() {
            return Err(CloudError::Corrupt(
                "local manifest bucket must not be empty".to_string(),
            ));
        }
        if self.cache_max_bytes == 0 {
            return Err(CloudError::Corrupt(
                "local manifest cache_max_bytes must be non-zero".to_string(),
            ));
        }
        if self.evict_low_watermark_bytes > self.cache_max_bytes {
            return Err(CloudError::Corrupt(
                "local manifest eviction low watermark exceeds cache size".to_string(),
            ));
        }
        Ok(())
    }
}
