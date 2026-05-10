pub mod local_mock;
#[cfg(feature = "s3")]
pub mod s3;

use crate::cloud::local_mock::LocalMockCloud;
#[cfg(feature = "s3")]
use crate::cloud::s3::S3Cloud;
use crate::error::{CloudError, Result};
use crate::util::chunk_file_name;
use crate::volume::manifest::{CloudManifest, LocalManifest};

pub trait CloudBackend {
    fn init(&self, manifest: &CloudManifest) -> Result<()>;
    fn describe(&self) -> String;
    fn fetch_chunk(&self, chunk_id: u64, expected_size: u64) -> Result<Option<Vec<u8>>>;
    fn upload_chunk(&self, chunk_id: u64, generation: u64, data: &[u8]) -> Result<String>;
}

#[derive(Debug)]
pub enum ConfiguredCloudBackend {
    LocalMock(LocalMockCloud),
    #[cfg(feature = "s3")]
    S3(S3Cloud),
}

impl ConfiguredCloudBackend {
    pub fn from_manifest(manifest: &LocalManifest) -> Result<Self> {
        match manifest.cloud_provider.as_str() {
            "local_mock" => Ok(Self::LocalMock(LocalMockCloud::new(&manifest.remote_dir))),
            "s3" => Self::s3_from_manifest(manifest),
            provider => Err(CloudError::Unsupported(format!(
                "cloud provider '{provider}'"
            ))),
        }
    }

    #[cfg(feature = "s3")]
    fn s3_from_manifest(manifest: &LocalManifest) -> Result<Self> {
        Ok(Self::S3(S3Cloud::new(
            manifest.bucket.clone(),
            manifest.prefix.clone(),
            manifest.region.clone(),
            manifest.endpoint_url.clone(),
        )?))
    }

    #[cfg(not(feature = "s3"))]
    fn s3_from_manifest(_manifest: &LocalManifest) -> Result<Self> {
        Err(CloudError::Unsupported(
            "S3 backend requires building with --features s3".to_string(),
        ))
    }
}

impl CloudBackend for ConfiguredCloudBackend {
    fn init(&self, manifest: &CloudManifest) -> Result<()> {
        match self {
            Self::LocalMock(backend) => backend.init(manifest),
            #[cfg(feature = "s3")]
            Self::S3(backend) => backend.init(manifest),
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::LocalMock(backend) => backend.describe(),
            #[cfg(feature = "s3")]
            Self::S3(backend) => backend.describe(),
        }
    }

    fn fetch_chunk(&self, chunk_id: u64, expected_size: u64) -> Result<Option<Vec<u8>>> {
        match self {
            Self::LocalMock(backend) => backend.fetch_chunk(chunk_id, expected_size),
            #[cfg(feature = "s3")]
            Self::S3(backend) => backend.fetch_chunk(chunk_id, expected_size),
        }
    }

    fn upload_chunk(&self, chunk_id: u64, generation: u64, data: &[u8]) -> Result<String> {
        match self {
            Self::LocalMock(backend) => backend.upload_chunk(chunk_id, generation, data),
            #[cfg(feature = "s3")]
            Self::S3(backend) => backend.upload_chunk(chunk_id, generation, data),
        }
    }
}

pub fn object_key(prefix: &str, object_name: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        object_name.to_string()
    } else {
        format!("{prefix}/{object_name}")
    }
}

pub fn chunk_object_key(prefix: &str, chunk_id: u64) -> String {
    object_key(prefix, &format!("chunks/{}", chunk_file_name(chunk_id)))
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "s3"))]
    use super::ConfiguredCloudBackend;
    use super::{chunk_object_key, object_key};
    #[cfg(not(feature = "s3"))]
    use crate::volume::manifest::{CloudConfig, LocalManifest};

    #[test]
    fn object_keys_trim_slashes() {
        assert_eq!(
            object_key("/prefix/", "manifest.json"),
            "prefix/manifest.json"
        );
        assert_eq!(object_key("", "manifest.json"), "manifest.json");
        assert_eq!(
            chunk_object_key("cloudcache/volumes/test", 7),
            "cloudcache/volumes/test/chunks/0000000000000007.chunk"
        );
    }

    #[cfg(not(feature = "s3"))]
    #[test]
    fn s3_backend_reports_missing_feature() {
        let manifest = LocalManifest::new(
            "test".to_string(),
            std::path::PathBuf::from("/tmp/cloudcache-test"),
            CloudConfig {
                provider: "s3".to_string(),
                bucket: "bucket".to_string(),
                prefix: "prefix".to_string(),
                remote_dir: std::path::PathBuf::new(),
                region: Some("us-east-1".to_string()),
                endpoint_url: None,
            },
            1024 * 1024,
        );

        let err = ConfiguredCloudBackend::from_manifest(&manifest).unwrap_err();
        assert!(err.to_string().contains("--features s3"));
    }
}
