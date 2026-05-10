use crate::error::{CloudError, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

pub fn now_rfc3339_utc() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{secs}")
}

pub fn parse_size(input: &str) -> Result<u64> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(CloudError::InvalidArgument("empty size".to_string()));
    }

    let split_at = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let number = &trimmed[..split_at];
    let suffix = trimmed[split_at..].trim().to_ascii_lowercase();
    let value = number
        .parse::<u64>()
        .map_err(|_| CloudError::InvalidArgument(format!("invalid size '{input}'")))?;
    let multiplier = match suffix.as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        other => {
            return Err(CloudError::InvalidArgument(format!(
                "unknown size suffix '{other}'"
            )));
        }
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| CloudError::InvalidArgument(format!("size too large '{input}'")))
}

pub fn human_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    fsync_parent(path)?;
    Ok(())
}

pub fn fsync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        let dir = File::open(parent)?;
        if let Err(err) = dir.sync_all()
            && err.kind() != std::io::ErrorKind::PermissionDenied
        {
            return Err(err.into());
        }
    }
    Ok(())
}

pub fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    Ok(())
}

pub fn open_rw_create(path: &Path) -> Result<File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?)
}

pub fn registry_path() -> PathBuf {
    if let Ok(path) = std::env::var("CLOUDCACHE_REGISTRY") {
        return PathBuf::from(path);
    }
    if let Ok(home) = std::env::var("HOME") {
        PathBuf::from(home).join(".cloudcache").join("volumes.tsv")
    } else {
        PathBuf::from(".cloudcache").join("volumes.tsv")
    }
}

pub fn chunk_file_name(chunk_id: u64) -> String {
    format!("{chunk_id:016x}.chunk")
}
