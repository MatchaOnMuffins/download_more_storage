use crate::error::{CloudError, Result};
use crate::util::{ensure_dir, fsync_parent, now_ns};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalEvent {
    WriteBegin {
        txid: u64,
        chunk_id: u64,
        offset_in_chunk: u64,
        length: u64,
        before_generation: u64,
        after_generation: u64,
    },
    LocalCommitted {
        txid: u64,
        chunk_id: u64,
        after_generation: u64,
    },
    CloudCommitted {
        chunk_id: u64,
        generation: u64,
        checksum: String,
    },
}

pub struct Journal {
    path: PathBuf,
    next_txid: u64,
}

impl Journal {
    pub fn open(dir: &Path) -> Result<Self> {
        ensure_dir(dir)?;
        let path = dir.join("current.wal");
        if !path.exists() {
            File::create(&path)?.sync_all()?;
        }
        let next_txid = Self::read_events_from(&path)?
            .iter()
            .filter_map(|event| match event {
                JournalEvent::WriteBegin { txid, .. }
                | JournalEvent::LocalCommitted { txid, .. } => Some(*txid),
                JournalEvent::CloudCommitted { .. } => None,
            })
            .max()
            .unwrap_or(0)
            + 1;
        Ok(Self { path, next_txid })
    }

    pub fn next_txid(&mut self) -> u64 {
        let txid = self.next_txid;
        self.next_txid += 1;
        txid
    }

    pub fn append(&self, event: &JournalEvent) -> Result<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)?;
        writeln!(file, "{}", encode_event(event))?;
        file.sync_all()?;
        Ok(())
    }

    pub fn read_events(&self) -> Result<Vec<JournalEvent>> {
        Self::read_events_from(&self.path)
    }

    pub fn read_events_from(path: &Path) -> Result<Vec<JournalEvent>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(path)?;
        let mut events = Vec::new();
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            events.push(decode_event(&line)?);
        }
        Ok(events)
    }

    pub fn compact(&self, events_to_keep: &[JournalEvent]) -> Result<()> {
        let tmp = self.path.with_extension(format!("{}.tmp", now_ns()));
        {
            let mut file = File::create(&tmp)?;
            for event in events_to_keep {
                writeln!(file, "{}", encode_event(event))?;
            }
            file.sync_all()?;
        }
        fs::rename(&tmp, &self.path)?;
        fsync_parent(&self.path)?;
        Ok(())
    }
}

fn encode_event(event: &JournalEvent) -> String {
    match event {
        JournalEvent::WriteBegin {
            txid,
            chunk_id,
            offset_in_chunk,
            length,
            before_generation,
            after_generation,
        } => format!(
            "type=write_begin txid={txid} chunk_id={chunk_id} offset_in_chunk={offset_in_chunk} length={length} before_generation={before_generation} after_generation={after_generation}"
        ),
        JournalEvent::LocalCommitted {
            txid,
            chunk_id,
            after_generation,
        } => format!(
            "type=local_committed txid={txid} chunk_id={chunk_id} after_generation={after_generation}"
        ),
        JournalEvent::CloudCommitted {
            chunk_id,
            generation,
            checksum,
        } => format!(
            "type=cloud_committed chunk_id={chunk_id} generation={generation} checksum={}",
            checksum.replace(' ', "%20")
        ),
    }
}

fn decode_event(line: &str) -> Result<JournalEvent> {
    let mut event_type = None;
    let mut txid = None;
    let mut chunk_id = None;
    let mut offset_in_chunk = None;
    let mut length = None;
    let mut before_generation = None;
    let mut after_generation = None;
    let mut generation = None;
    let mut checksum = None;

    for part in line.split_whitespace() {
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| CloudError::Corrupt(format!("invalid journal token '{part}'")))?;
        match key {
            "type" => event_type = Some(value.to_string()),
            "txid" => txid = Some(parse_u64(key, value)?),
            "chunk_id" => chunk_id = Some(parse_u64(key, value)?),
            "offset_in_chunk" => offset_in_chunk = Some(parse_u64(key, value)?),
            "length" => length = Some(parse_u64(key, value)?),
            "before_generation" => before_generation = Some(parse_u64(key, value)?),
            "after_generation" => after_generation = Some(parse_u64(key, value)?),
            "generation" => generation = Some(parse_u64(key, value)?),
            "checksum" => checksum = Some(value.replace("%20", " ")),
            _ => {}
        }
    }

    match event_type.as_deref() {
        Some("write_begin") => Ok(JournalEvent::WriteBegin {
            txid: txid.ok_or_else(|| missing("txid"))?,
            chunk_id: chunk_id.ok_or_else(|| missing("chunk_id"))?,
            offset_in_chunk: offset_in_chunk.ok_or_else(|| missing("offset_in_chunk"))?,
            length: length.ok_or_else(|| missing("length"))?,
            before_generation: before_generation.ok_or_else(|| missing("before_generation"))?,
            after_generation: after_generation.ok_or_else(|| missing("after_generation"))?,
        }),
        Some("local_committed") => Ok(JournalEvent::LocalCommitted {
            txid: txid.ok_or_else(|| missing("txid"))?,
            chunk_id: chunk_id.ok_or_else(|| missing("chunk_id"))?,
            after_generation: after_generation.ok_or_else(|| missing("after_generation"))?,
        }),
        Some("cloud_committed") => Ok(JournalEvent::CloudCommitted {
            chunk_id: chunk_id.ok_or_else(|| missing("chunk_id"))?,
            generation: generation.ok_or_else(|| missing("generation"))?,
            checksum: checksum.ok_or_else(|| missing("checksum"))?,
        }),
        other => Err(CloudError::Corrupt(format!(
            "unknown journal event type '{other:?}'"
        ))),
    }
}

fn parse_u64(key: &str, value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|_| CloudError::Corrupt(format!("invalid u64 for {key}: '{value}'")))
}

fn missing(key: &str) -> CloudError {
    CloudError::Corrupt(format!("missing journal key '{key}'"))
}
