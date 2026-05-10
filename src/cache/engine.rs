use crate::checksum::sha256_hex;
use crate::cloud::{CloudBackend, ConfiguredCloudBackend};
use crate::error::{CloudError, Result};
use crate::journal::wal::{Journal, JournalEvent};
use crate::metadata::{ChunkMeta, ChunkState, MetadataDb, MetadataStats};
use crate::util::{chunk_file_name, ensure_dir, fsync_parent, now_ns, open_rw_create};
use crate::volume::manifest::{CloudManifest, LocalManifest};
use crate::volume::mapper;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct CacheEngine {
    pub cloud_manifest: CloudManifest,
    pub local_manifest: LocalManifest,
    db: MetadataDb,
    journal: Journal,
    cloud: ConfiguredCloudBackend,
}

#[derive(Debug, Clone)]
pub struct VolumeStatus {
    pub volume_id: String,
    pub volume_size_bytes: u64,
    pub chunk_size_bytes: u64,
    pub cache_max_bytes: u64,
    pub cloud_backend: String,
    pub metadata: MetadataStats,
}

impl CacheEngine {
    pub fn create(cloud_manifest: CloudManifest, local_manifest: LocalManifest) -> Result<Self> {
        cloud_manifest.validate()?;
        local_manifest.validate()?;
        if cloud_manifest.volume_id != local_manifest.volume_id {
            return Err(CloudError::InvalidArgument(
                "cloud and local manifests must use the same volume id".to_string(),
            ));
        }
        ensure_dir(&local_manifest.cache_dir)?;
        ensure_dir(&local_manifest.cache_dir.join("chunks"))?;
        ensure_dir(&local_manifest.cache_dir.join("journal"))?;
        ensure_dir(&local_manifest.cache_dir.join("tmp"))?;

        let cloud = ConfiguredCloudBackend::from_manifest(&local_manifest)?;
        cloud.init(&cloud_manifest)?;
        cloud_manifest.write(&local_manifest.cache_dir.join("manifest.json"))?;
        local_manifest.write()?;

        let db = MetadataDb::open(local_manifest.cache_dir.join("metadata.sqlite"))?;
        let journal = Journal::open(&local_manifest.cache_dir.join("journal"))?;
        Ok(Self {
            cloud_manifest,
            local_manifest,
            db,
            journal,
            cloud,
        })
    }

    pub fn open(cache_dir: &Path) -> Result<Self> {
        let local_manifest = LocalManifest::read(cache_dir)?;
        let cloud_manifest = CloudManifest::read(&local_manifest.cache_dir.join("manifest.json"))?;
        if cloud_manifest.volume_id != local_manifest.volume_id {
            return Err(CloudError::Corrupt(format!(
                "cloud manifest volume '{}' does not match local manifest volume '{}'",
                cloud_manifest.volume_id, local_manifest.volume_id
            )));
        }
        let db = MetadataDb::open(local_manifest.cache_dir.join("metadata.sqlite"))?;
        let journal = Journal::open(&local_manifest.cache_dir.join("journal"))?;
        let cloud = ConfiguredCloudBackend::from_manifest(&local_manifest)?;
        let mut engine = Self {
            cloud_manifest,
            local_manifest,
            db,
            journal,
            cloud,
        };
        engine.recover()?;
        Ok(engine)
    }

    pub fn metadata_path(&self) -> &Path {
        self.db.path()
    }

    pub fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let spans = mapper::split(
            offset,
            len,
            self.cloud_manifest.chunk_size_bytes,
            self.cloud_manifest.volume_size_bytes,
        )?;
        let mut out = vec![0u8; len];
        for span in spans {
            let path = self.ensure_chunk_for_read(span.chunk_id)?;
            let mut file = File::open(path)?;
            file.seek(SeekFrom::Start(span.offset_in_chunk))?;
            file.read_exact(&mut out[span.request_offset..span.request_offset + span.len])?;
            self.db.update_access(span.chunk_id, now_ns())?;
        }
        Ok(out)
    }

    pub fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let spans = mapper::split(
            offset,
            data.len(),
            self.cloud_manifest.chunk_size_bytes,
            self.cloud_manifest.volume_size_bytes,
        )?;

        for span in spans {
            let span_data = &data[span.request_offset..span.request_offset + span.len];
            let chunk_size = self.chunk_len(span.chunk_id);
            let full_chunk = span.offset_in_chunk == 0 && span.len as u64 == chunk_size;
            let path = if full_chunk {
                self.prepare_empty_chunk(span.chunk_id)?
            } else {
                self.ensure_chunk_for_read(span.chunk_id)?
            };

            let before_generation = self
                .db
                .get_chunk(span.chunk_id)?
                .and_then(|meta| meta.local_generation)
                .unwrap_or(0);
            let after_generation = before_generation + 1;
            let txid = self.journal.next_txid();

            self.journal.append(&JournalEvent::WriteBegin {
                txid,
                chunk_id: span.chunk_id,
                offset_in_chunk: span.offset_in_chunk,
                length: span.len as u64,
                before_generation,
                after_generation,
            })?;

            let mut file = open_rw_create(&path)?;
            file.seek(SeekFrom::Start(span.offset_in_chunk))?;
            file.write_all(span_data)?;
            file.set_len(chunk_size)?;
            file.sync_all()?;

            let meta = ChunkMeta {
                chunk_id: span.chunk_id,
                local_path: Some(path.clone()),
                state: ChunkState::Dirty,
                size_bytes: chunk_size,
                remote_generation: None,
                local_generation: Some(after_generation),
                checksum: None,
                last_access_ns: now_ns(),
                dirty_since_ns: Some(now_ns()),
                pin_count: 0,
            };
            self.db.upsert_chunk(&meta)?;
            self.db.exec("PRAGMA wal_checkpoint(FULL);")?;
            self.journal.append(&JournalEvent::LocalCommitted {
                txid,
                chunk_id: span.chunk_id,
                after_generation,
            })?;
        }
        self.evict_if_needed()?;
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.db.exec("PRAGMA wal_checkpoint(FULL);")?;
        Ok(())
    }

    pub fn sync_dirty(&mut self) -> Result<u64> {
        let mut uploaded_bytes = 0;
        for mut meta in self.db.dirty_chunks()? {
            let Some(path) = meta.local_path.clone() else {
                return Err(CloudError::Corrupt(format!(
                    "dirty chunk {} has no local path",
                    meta.chunk_id
                )));
            };
            let data = match fs::read(&path) {
                Ok(data) => data,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    meta.state = ChunkState::Error;
                    meta.local_path = None;
                    meta.size_bytes = 0;
                    meta.dirty_since_ns = None;
                    self.db.upsert_chunk(&meta)?;
                    return Err(CloudError::Corrupt(format!(
                        "dirty chunk {} has no local file",
                        meta.chunk_id
                    )));
                }
                Err(err) => return Err(err.into()),
            };
            let generation = meta.local_generation.unwrap_or(0);
            meta.state = ChunkState::Uploading;
            self.db.upsert_chunk(&meta)?;
            let checksum = match self.cloud.upload_chunk(meta.chunk_id, generation, &data) {
                Ok(checksum) => checksum,
                Err(err) => {
                    meta.state = ChunkState::Dirty;
                    self.db.upsert_chunk(&meta)?;
                    return Err(err);
                }
            };
            meta.state = ChunkState::Clean;
            meta.remote_generation = Some(generation);
            meta.checksum = Some(checksum.clone());
            meta.dirty_since_ns = None;
            self.db.upsert_chunk(&meta)?;
            self.journal.append(&JournalEvent::CloudCommitted {
                chunk_id: meta.chunk_id,
                generation,
                checksum,
            })?;
            uploaded_bytes += data.len() as u64;
        }
        self.evict_if_needed()?;
        Ok(uploaded_bytes)
    }

    pub fn evict_if_needed(&mut self) -> Result<u64> {
        let total_chunks = self.total_chunks();
        let mut stats = self.db.stats(total_chunks)?;
        if stats.cached_bytes <= self.local_manifest.cache_max_bytes {
            return Ok(0);
        }

        let mut evicted = 0;
        for meta in self.db.evictable_clean_chunks()? {
            if stats.cached_bytes <= self.local_manifest.evict_low_watermark_bytes {
                break;
            }
            if let Some(path) = &meta.local_path
                && path.exists()
            {
                fs::remove_file(path)?;
                fsync_parent(path)?;
            }
            let evicted_bytes = meta.size_bytes;
            let missing = ChunkMeta {
                local_path: None,
                state: ChunkState::Missing,
                size_bytes: 0,
                checksum: meta.checksum,
                last_access_ns: now_ns(),
                dirty_since_ns: None,
                ..meta
            };
            self.db.upsert_chunk(&missing)?;
            stats.cached_bytes = stats.cached_bytes.saturating_sub(evicted_bytes);
            evicted += 1;
        }
        Ok(evicted)
    }

    pub fn status(&self) -> Result<VolumeStatus> {
        Ok(VolumeStatus {
            volume_id: self.cloud_manifest.volume_id.clone(),
            volume_size_bytes: self.cloud_manifest.volume_size_bytes,
            chunk_size_bytes: self.cloud_manifest.chunk_size_bytes,
            cache_max_bytes: self.local_manifest.cache_max_bytes,
            cloud_backend: self.cloud.describe(),
            metadata: self.db.stats(self.total_chunks())?,
        })
    }

    pub fn fsck(&mut self) -> Result<Vec<String>> {
        let mut findings = Vec::new();
        for mut meta in self.db.all_chunks()? {
            match meta.state {
                ChunkState::Clean
                | ChunkState::Dirty
                | ChunkState::Uploading
                | ChunkState::Fetching => {
                    let missing_file = meta
                        .local_path
                        .as_ref()
                        .map(|path| !path.exists())
                        .unwrap_or(true);
                    if missing_file {
                        findings.push(format!(
                            "chunk {} had state {} but no local file; marking {}",
                            meta.chunk_id,
                            meta.state.as_str(),
                            if matches!(meta.state, ChunkState::Dirty | ChunkState::Uploading) {
                                ChunkState::Error.as_str()
                            } else {
                                ChunkState::Missing.as_str()
                            }
                        ));
                        if matches!(meta.state, ChunkState::Dirty | ChunkState::Uploading) {
                            meta.state = ChunkState::Error;
                        } else {
                            meta.state = ChunkState::Missing;
                        }
                        meta.local_path = None;
                        meta.size_bytes = 0;
                        meta.dirty_since_ns = None;
                        self.db.upsert_chunk(&meta)?;
                    }
                }
                ChunkState::Missing | ChunkState::Error => {}
            }
        }
        self.recover()?;
        Ok(findings)
    }

    pub fn recover(&mut self) -> Result<()> {
        self.validate_local_chunk_files()?;

        let mut latest_local: HashMap<u64, u64> = HashMap::new();
        let mut cloud_committed: HashSet<(u64, u64)> = HashSet::new();

        for event in self.journal.read_events()? {
            match event {
                JournalEvent::LocalCommitted {
                    chunk_id,
                    after_generation,
                    ..
                } => {
                    latest_local
                        .entry(chunk_id)
                        .and_modify(|generation| *generation = (*generation).max(after_generation))
                        .or_insert(after_generation);
                }
                JournalEvent::CloudCommitted {
                    chunk_id,
                    generation,
                    ..
                } => {
                    cloud_committed.insert((chunk_id, generation));
                }
                JournalEvent::WriteBegin { .. } => {}
            }
        }

        for (chunk_id, generation) in latest_local {
            let path = self.chunk_path(chunk_id);
            if !path.exists() {
                continue;
            }
            let is_cloud_committed = cloud_committed.contains(&(chunk_id, generation));
            let data = fs::read(&path)?;
            let meta = ChunkMeta {
                chunk_id,
                local_path: Some(path),
                state: if is_cloud_committed {
                    ChunkState::Clean
                } else {
                    ChunkState::Dirty
                },
                size_bytes: data.len() as u64,
                remote_generation: if is_cloud_committed {
                    Some(generation)
                } else {
                    None
                },
                local_generation: Some(generation),
                checksum: Some(sha256_hex(&data)),
                last_access_ns: now_ns(),
                dirty_since_ns: if is_cloud_committed {
                    None
                } else {
                    Some(now_ns())
                },
                pin_count: 0,
            };
            self.db.upsert_chunk(&meta)?;
        }
        Ok(())
    }

    fn validate_local_chunk_files(&self) -> Result<()> {
        for mut meta in self.db.all_chunks()? {
            let should_have_file = matches!(
                meta.state,
                ChunkState::Clean
                    | ChunkState::Dirty
                    | ChunkState::Uploading
                    | ChunkState::Fetching
            );
            if !should_have_file {
                continue;
            }
            let missing_file = meta
                .local_path
                .as_ref()
                .map(|path| !path.exists())
                .unwrap_or(true);
            if !missing_file {
                continue;
            }

            if matches!(meta.state, ChunkState::Dirty | ChunkState::Uploading) {
                meta.state = ChunkState::Error;
            } else {
                meta.state = ChunkState::Missing;
            }
            meta.local_path = None;
            meta.size_bytes = 0;
            meta.dirty_since_ns = None;
            self.db.upsert_chunk(&meta)?;
        }
        Ok(())
    }

    fn ensure_chunk_for_read(&mut self, chunk_id: u64) -> Result<PathBuf> {
        if let Some(meta) = self.db.get_chunk(chunk_id)? {
            if meta.state == ChunkState::Error {
                return Err(CloudError::Corrupt(format!(
                    "chunk {chunk_id} is in error state; run fsck and restore from a valid backup"
                )));
            }
            if matches!(meta.state, ChunkState::Clean | ChunkState::Dirty)
                && meta
                    .local_path
                    .as_ref()
                    .map(|p| p.exists())
                    .unwrap_or(false)
            {
                return Ok(meta.local_path.unwrap());
            }
        }

        let path = self.chunk_path(chunk_id);
        let chunk_size = self.chunk_len(chunk_id);
        let data = self
            .cloud
            .fetch_chunk(chunk_id, chunk_size)?
            .unwrap_or_else(|| vec![0u8; chunk_size as usize]);

        let tmp = self.local_manifest.cache_dir.join("tmp").join(format!(
            "{}.{}.tmp",
            chunk_file_name(chunk_id),
            now_ns()
        ));
        {
            let mut file = File::create(&tmp)?;
            file.write_all(&data)?;
            file.set_len(chunk_size)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        fsync_parent(&path)?;
        let checksum = sha256_hex(&data);
        self.db.upsert_chunk(&ChunkMeta {
            chunk_id,
            local_path: Some(path.clone()),
            state: ChunkState::Clean,
            size_bytes: chunk_size,
            remote_generation: None,
            local_generation: None,
            checksum: Some(checksum),
            last_access_ns: now_ns(),
            dirty_since_ns: None,
            pin_count: 0,
        })?;
        self.evict_if_needed()?;
        Ok(path)
    }

    fn prepare_empty_chunk(&mut self, chunk_id: u64) -> Result<PathBuf> {
        let path = self.chunk_path(chunk_id);
        if let Some(parent) = path.parent() {
            ensure_dir(parent)?;
        }
        let file = File::create(&path)?;
        file.set_len(self.chunk_len(chunk_id))?;
        file.sync_all()?;
        fsync_parent(&path)?;
        Ok(path)
    }

    fn chunk_path(&self, chunk_id: u64) -> PathBuf {
        self.local_manifest
            .cache_dir
            .join("chunks")
            .join(chunk_file_name(chunk_id))
    }

    fn chunk_len(&self, chunk_id: u64) -> u64 {
        let start = chunk_id * self.cloud_manifest.chunk_size_bytes;
        let remaining = self.cloud_manifest.volume_size_bytes.saturating_sub(start);
        remaining.min(self.cloud_manifest.chunk_size_bytes)
    }

    fn total_chunks(&self) -> u64 {
        self.cloud_manifest
            .volume_size_bytes
            .div_ceil(self.cloud_manifest.chunk_size_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::CacheEngine;
    use crate::volume::manifest::{CloudConfig, CloudManifest, LocalManifest};
    use std::fs;
    use std::path::PathBuf;

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cloudcache-test-{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn engine(name: &str, cache_max_bytes: u64) -> CacheEngine {
        let root = temp_root(name);
        let cache_dir = root.join("cache");
        let remote_dir = root.join("remote");
        CacheEngine::create(
            CloudManifest::new("test".to_string(), 1024 * 1024, 64 * 1024),
            LocalManifest::new(
                "test".to_string(),
                cache_dir,
                CloudConfig {
                    provider: "local_mock".to_string(),
                    bucket: "mock".to_string(),
                    prefix: "cloudcache/volumes/test".to_string(),
                    remote_dir,
                    region: None,
                    endpoint_url: None,
                },
                cache_max_bytes,
            ),
        )
        .unwrap()
    }

    #[test]
    fn missing_reads_return_zeroes() {
        let mut engine = engine("zero", 1024 * 1024);
        assert_eq!(engine.read_at(1234, 32).unwrap(), vec![0u8; 32]);
    }

    #[test]
    fn write_read_across_chunks() {
        let mut engine = engine("cross", 1024 * 1024);
        let data = vec![7u8; 8192];
        engine.write_at(64 * 1024 - 16, &data).unwrap();
        assert_eq!(engine.read_at(64 * 1024 - 16, data.len()).unwrap(), data);
    }

    #[test]
    fn sync_and_refetch_from_mock_cloud() {
        let mut engine = engine("sync", 1024 * 1024);
        let cache_dir = engine.local_manifest.cache_dir.clone();
        let data = b"hello cloud backed block cache".to_vec();
        engine.write_at(4096, &data).unwrap();
        assert!(engine.sync_dirty().unwrap() > 0);
        let chunk_path = cache_dir.join("chunks").join("0000000000000000.chunk");
        fs::remove_file(chunk_path).unwrap();
        let mut reopened = CacheEngine::open(&cache_dir).unwrap();
        reopened.fsck().unwrap();
        assert_eq!(reopened.read_at(4096, data.len()).unwrap(), data);
    }

    #[test]
    fn missing_dirty_chunk_is_error_not_zero_filled() {
        let mut engine = engine("missing-dirty", 1024 * 1024);
        let data = b"dirty data that only exists locally".to_vec();
        engine.write_at(4096, &data).unwrap();

        let chunk_path = engine
            .local_manifest
            .cache_dir
            .join("chunks")
            .join("0000000000000000.chunk");
        fs::remove_file(chunk_path).unwrap();

        let findings = engine.fsck().unwrap();
        assert!(
            findings
                .iter()
                .any(|finding| finding.contains("marking error"))
        );
        assert_eq!(
            engine.db.get_chunk(0).unwrap().unwrap().state,
            crate::metadata::ChunkState::Error
        );
        assert!(engine.read_at(4096, data.len()).is_err());
    }
}
