use crate::checksum::sha256_hex;
use crate::cloud::{CloudBackend, chunk_object_key, object_key};
use crate::error::{CloudError, Result};
use crate::volume::manifest::CloudManifest;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use tokio::runtime::Runtime;

#[derive(Debug)]
pub struct S3Cloud {
    client: Client,
    runtime: Runtime,
    bucket: String,
    prefix: String,
}

impl S3Cloud {
    pub fn new(
        bucket: String,
        prefix: String,
        region: Option<String>,
        endpoint_url: Option<String>,
    ) -> Result<Self> {
        if bucket.is_empty() {
            return Err(CloudError::InvalidArgument(
                "S3 backend requires a bucket".to_string(),
            ));
        }

        let runtime = Runtime::new()?;
        let client = runtime.block_on(async {
            let mut config_loader = aws_config::defaults(BehaviorVersion::latest());
            if let Some(region) = region {
                config_loader = config_loader.region(Region::new(region));
            }
            if let Some(endpoint_url) = endpoint_url {
                config_loader = config_loader.endpoint_url(endpoint_url);
            }
            let config = config_loader.load().await;
            Client::new(&config)
        });

        Ok(Self {
            client,
            runtime,
            bucket,
            prefix,
        })
    }

    fn manifest_key(&self) -> String {
        object_key(&self.prefix, "manifest.json")
    }

    fn chunk_key(&self, chunk_id: u64) -> String {
        chunk_object_key(&self.prefix, chunk_id)
    }
}

impl CloudBackend for S3Cloud {
    fn init(&self, manifest: &CloudManifest) -> Result<()> {
        let body = serde_json::to_vec_pretty(manifest)
            .map_err(|err| CloudError::Corrupt(format!("serialize cloud manifest: {err}")))?;
        self.runtime.block_on(async {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(self.manifest_key())
                .body(ByteStream::from(body))
                .send()
                .await
                .map_err(|err| {
                    CloudError::Io(std::io::Error::other(format!("S3 put manifest: {err}")))
                })
                .map(|_| ())
        })
    }

    fn describe(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix.trim_matches('/'))
    }

    fn fetch_chunk(&self, chunk_id: u64, expected_size: u64) -> Result<Option<Vec<u8>>> {
        let key = self.chunk_key(chunk_id);
        self.runtime.block_on(async {
            let output = match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
            {
                Ok(output) => output,
                Err(err)
                    if err
                        .as_service_error()
                        .is_some_and(|err| err.is_no_such_key()) =>
                {
                    return Ok(None);
                }
                Err(err) => {
                    return Err(CloudError::Io(std::io::Error::other(format!(
                        "S3 get object {key}: {err}"
                    ))));
                }
            };

            let data = output
                .body
                .collect()
                .await
                .map_err(|err| {
                    CloudError::Io(std::io::Error::other(format!(
                        "S3 read object body {key}: {err}"
                    )))
                })?
                .into_bytes()
                .to_vec();

            if data.len() as u64 != expected_size {
                return Err(CloudError::Corrupt(format!(
                    "S3 object {key} size {} does not match expected size {expected_size}",
                    data.len()
                )));
            }

            if let Some(checksum) = output
                .metadata
                .and_then(|mut metadata| metadata.remove("cloudcache-sha256"))
                && checksum != sha256_hex(&data)
            {
                return Err(CloudError::Corrupt(format!(
                    "S3 object {key} checksum does not match metadata"
                )));
            }

            Ok(Some(data))
        })
    }

    fn upload_chunk(&self, chunk_id: u64, generation: u64, data: &[u8]) -> Result<String> {
        let key = self.chunk_key(chunk_id);
        let checksum = sha256_hex(data);
        let body = data.to_vec();
        let checksum_for_metadata = checksum.clone();
        self.runtime.block_on(async {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .metadata("cloudcache-sha256", checksum_for_metadata)
                .metadata("cloudcache-generation", generation.to_string())
                .body(ByteStream::from(body))
                .send()
                .await
                .map_err(|err| {
                    CloudError::Io(std::io::Error::other(format!("S3 put object {key}: {err}")))
                })?;
            Ok(checksum)
        })
    }
}
