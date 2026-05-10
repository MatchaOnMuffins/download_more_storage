#![cfg(feature = "s3")]

use cloudcache::cache::engine::CacheEngine;
use cloudcache::volume::manifest::{CloudConfig, CloudManifest, LocalManifest};
use std::path::PathBuf;

#[test]
#[ignore = "requires real S3 credentials and CLOUDCACHE_S3_TEST_BUCKET"]
fn s3_sync_and_refetch_smoke() {
    let bucket =
        std::env::var("CLOUDCACHE_S3_TEST_BUCKET").expect("CLOUDCACHE_S3_TEST_BUCKET must be set");
    let region = std::env::var("AWS_REGION").ok();
    let endpoint_url = std::env::var("CLOUDCACHE_S3_ENDPOINT_URL").ok();
    let remote = S3TestRemote {
        bucket: bucket.clone(),
        region: region.clone(),
        endpoint_url: endpoint_url.clone(),
    };
    let unique = format!(
        "s3-smoke-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let root = std::env::temp_dir().join(format!("cloudcache-{unique}"));
    let cache_dir = root.join("cache");
    let prefix = format!("cloudcache/test/{unique}");

    let mut engine = CacheEngine::create(
        CloudManifest::new(unique.clone(), 8 * 1024 * 1024, 1024 * 1024),
        LocalManifest::new(
            unique,
            cache_dir.clone(),
            CloudConfig {
                provider: "s3".to_string(),
                bucket,
                prefix: prefix.clone(),
                remote_dir: PathBuf::new(),
                region,
                endpoint_url,
            },
            8 * 1024 * 1024,
        ),
    )
    .unwrap();

    let data = b"cloudcache s3 smoke test data".to_vec();
    engine.write_at(4096, &data).unwrap();
    assert!(engine.sync_dirty().unwrap() > 0);

    let chunk_path = cache_dir.join("chunks").join("0000000000000000.chunk");
    std::fs::remove_file(chunk_path).unwrap();
    drop(engine);

    let mut reopened = CacheEngine::open(&cache_dir).unwrap();
    reopened.fsck().unwrap();
    assert_eq!(reopened.read_at(4096, data.len()).unwrap(), data);

    remote.delete_object(&format!("{prefix}/manifest.json"));
    remote.delete_object(&format!("{prefix}/chunks/0000000000000000.chunk"));
    let _ = std::fs::remove_dir_all(root);
}

struct S3TestRemote {
    bucket: String,
    region: Option<String>,
    endpoint_url: Option<String>,
}

impl S3TestRemote {
    fn delete_object(&self, key: &str) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
            if let Some(region) = &self.region {
                config_loader = config_loader.region(aws_config::Region::new(region.clone()));
            }
            if let Some(endpoint_url) = &self.endpoint_url {
                config_loader = config_loader.endpoint_url(endpoint_url);
            }
            let config = config_loader.load().await;
            let client = aws_sdk_s3::Client::new(&config);
            client
                .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .unwrap();
        });
    }
}
