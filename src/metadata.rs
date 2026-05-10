use crate::error::{CloudError, Result};
use rusqlite::{Connection, OptionalExtension, Row, params};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkState {
    Missing,
    Fetching,
    Clean,
    Dirty,
    Uploading,
    Error,
}

impl ChunkState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Fetching => "fetching",
            Self::Clean => "clean",
            Self::Dirty => "dirty",
            Self::Uploading => "uploading",
            Self::Error => "error",
        }
    }
}

impl TryFrom<&str> for ChunkState {
    type Error = CloudError;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "missing" => Ok(Self::Missing),
            "fetching" => Ok(Self::Fetching),
            "clean" => Ok(Self::Clean),
            "dirty" => Ok(Self::Dirty),
            "uploading" => Ok(Self::Uploading),
            "error" => Ok(Self::Error),
            other => Err(CloudError::Corrupt(format!(
                "unknown chunk state '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChunkMeta {
    pub chunk_id: u64,
    pub local_path: Option<PathBuf>,
    pub state: ChunkState,
    pub size_bytes: u64,
    pub remote_generation: Option<u64>,
    pub local_generation: Option<u64>,
    pub checksum: Option<String>,
    pub last_access_ns: u128,
    pub dirty_since_ns: Option<u128>,
    pub pin_count: u64,
}

#[derive(Debug, Clone)]
pub struct MetadataStats {
    pub cached_bytes: u64,
    pub clean_bytes: u64,
    pub dirty_bytes: u64,
    pub cached_chunks: u64,
    pub missing_chunks: u64,
}

pub struct MetadataDb {
    path: PathBuf,
    conn: Connection,
}

impl MetadataDb {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let conn = Connection::open(&path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id INTEGER PRIMARY KEY,
                local_path TEXT,
                state TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                remote_generation INTEGER,
                local_generation INTEGER,
                checksum TEXT,
                last_access_ns INTEGER,
                dirty_since_ns INTEGER,
                pin_count INTEGER NOT NULL DEFAULT 0
            );
            ",
        )?;
        Ok(Self { path, conn })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn get_chunk(&self, chunk_id: u64) -> Result<Option<ChunkMeta>> {
        self.conn
            .query_row(
                "
                SELECT chunk_id, local_path, state, size_bytes, remote_generation,
                       local_generation, checksum, last_access_ns, dirty_since_ns, pin_count
                FROM chunks
                WHERE chunk_id = ?1;
                ",
                params![to_i64(chunk_id, "chunk_id")?],
                chunk_meta_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn upsert_chunk(&self, meta: &ChunkMeta) -> Result<()> {
        let local_path = meta
            .local_path
            .as_ref()
            .map(|path| path.display().to_string());
        self.conn.execute(
            "
            INSERT INTO chunks (
                chunk_id, local_path, state, size_bytes, remote_generation, local_generation,
                checksum, last_access_ns, dirty_since_ns, pin_count
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(chunk_id) DO UPDATE SET
                local_path = excluded.local_path,
                state = excluded.state,
                size_bytes = excluded.size_bytes,
                remote_generation = excluded.remote_generation,
                local_generation = excluded.local_generation,
                checksum = excluded.checksum,
                last_access_ns = excluded.last_access_ns,
                dirty_since_ns = excluded.dirty_since_ns,
                pin_count = excluded.pin_count;
            ",
            params![
                to_i64(meta.chunk_id, "chunk_id")?,
                local_path,
                meta.state.as_str(),
                to_i64(meta.size_bytes, "size_bytes")?,
                opt_to_i64(meta.remote_generation, "remote_generation")?,
                opt_to_i64(meta.local_generation, "local_generation")?,
                meta.checksum,
                u128_to_i64(meta.last_access_ns, "last_access_ns")?,
                opt_u128_to_i64(meta.dirty_since_ns, "dirty_since_ns")?,
                to_i64(meta.pin_count, "pin_count")?,
            ],
        )?;
        Ok(())
    }

    pub fn update_access(&self, chunk_id: u64, last_access_ns: u128) -> Result<()> {
        self.conn.execute(
            "UPDATE chunks SET last_access_ns = ?1 WHERE chunk_id = ?2;",
            params![
                u128_to_i64(last_access_ns, "last_access_ns")?,
                to_i64(chunk_id, "chunk_id")?,
            ],
        )?;
        Ok(())
    }

    pub fn dirty_chunks(&self) -> Result<Vec<ChunkMeta>> {
        self.query_chunks("WHERE state = 'dirty' ORDER BY dirty_since_ns ASC, chunk_id ASC")
    }

    pub fn evictable_clean_chunks(&self) -> Result<Vec<ChunkMeta>> {
        self.query_chunks(
            "WHERE state = 'clean' AND pin_count = 0 ORDER BY last_access_ns ASC, chunk_id ASC",
        )
    }

    pub fn all_chunks(&self) -> Result<Vec<ChunkMeta>> {
        self.query_chunks("ORDER BY chunk_id ASC")
    }

    pub fn stats(&self, total_chunks: u64) -> Result<MetadataStats> {
        let mut stmt = self.conn.prepare(
            "SELECT state, COUNT(*), IFNULL(SUM(size_bytes), 0) FROM chunks GROUP BY state;",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;

        let mut cached_bytes = 0;
        let mut clean_bytes = 0;
        let mut dirty_bytes = 0;
        let mut cached_chunks = 0;
        for row in rows {
            let (state, count, bytes) = row?;
            let count = from_i64(count, "count")?;
            let bytes = from_i64(bytes, "bytes")?;
            match state.as_str() {
                "clean" => {
                    clean_bytes += bytes;
                    cached_bytes += bytes;
                    cached_chunks += count;
                }
                "dirty" | "uploading" => {
                    dirty_bytes += bytes;
                    cached_bytes += bytes;
                    cached_chunks += count;
                }
                "fetching" => {
                    cached_bytes += bytes;
                    cached_chunks += count;
                }
                "missing" => {}
                _ => {}
            }
        }
        let missing_chunks = total_chunks.saturating_sub(cached_chunks);
        Ok(MetadataStats {
            cached_bytes,
            clean_bytes,
            dirty_bytes,
            cached_chunks,
            missing_chunks,
        })
    }

    pub fn exec(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql)?;
        Ok(())
    }

    fn query_chunks(&self, suffix: &str) -> Result<Vec<ChunkMeta>> {
        let sql = format!(
            "
            SELECT chunk_id, local_path, state, size_bytes, remote_generation,
                   local_generation, checksum, last_access_ns, dirty_since_ns, pin_count
            FROM chunks
            {suffix};
            "
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], chunk_meta_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

fn chunk_meta_from_row(row: &Row<'_>) -> rusqlite::Result<ChunkMeta> {
    let chunk_id = row.get::<_, i64>(0)?;
    let local_path = row.get::<_, Option<String>>(1)?;
    let state = row.get::<_, String>(2)?;
    let size_bytes = row.get::<_, i64>(3)?;
    let remote_generation = row.get::<_, Option<i64>>(4)?;
    let local_generation = row.get::<_, Option<i64>>(5)?;
    let checksum = row.get::<_, Option<String>>(6)?;
    let last_access_ns = row.get::<_, Option<i64>>(7)?.unwrap_or(0);
    let dirty_since_ns = row.get::<_, Option<i64>>(8)?;
    let pin_count = row.get::<_, i64>(9)?;

    Ok(ChunkMeta {
        chunk_id: from_i64_for_row(chunk_id),
        local_path: local_path.map(PathBuf::from),
        state: ChunkState::try_from(state.as_str()).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(err))
        })?,
        size_bytes: from_i64_for_row(size_bytes),
        remote_generation: remote_generation.map(from_i64_for_row),
        local_generation: local_generation.map(from_i64_for_row),
        checksum,
        last_access_ns: last_access_ns as u128,
        dirty_since_ns: dirty_since_ns.map(|value| value as u128),
        pin_count: from_i64_for_row(pin_count),
    })
}

fn to_i64(value: u64, name: &str) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| CloudError::InvalidArgument(format!("{name} exceeds SQLite INTEGER range")))
}

fn opt_to_i64(value: Option<u64>, name: &str) -> Result<Option<i64>> {
    value.map(|value| to_i64(value, name)).transpose()
}

fn u128_to_i64(value: u128, name: &str) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| CloudError::InvalidArgument(format!("{name} exceeds SQLite INTEGER range")))
}

fn opt_u128_to_i64(value: Option<u128>, name: &str) -> Result<Option<i64>> {
    value.map(|value| u128_to_i64(value, name)).transpose()
}

fn from_i64(value: i64, name: &str) -> Result<u64> {
    u64::try_from(value)
        .map_err(|_| CloudError::Corrupt(format!("{name} is negative in metadata database")))
}

fn from_i64_for_row(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}
