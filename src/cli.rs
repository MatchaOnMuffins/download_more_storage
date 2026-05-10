use crate::cache::engine::{CacheEngine, VolumeStatus};
use crate::device;
use crate::error::{CloudError, Result};
use crate::util::{atomic_write, default_volume_cache_dir, human_size, parse_size, registry_path};
use crate::volume::manifest::{CloudManifest, DEFAULT_SECTOR_SIZE_BYTES, LocalManifest};
use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum CliError {
    Clap(clap::Error),
    Cloud(CloudError),
}

impl From<CloudError> for CliError {
    fn from(err: CloudError) -> Self {
        Self::Cloud(err)
    }
}

#[derive(Debug, Parser)]
#[command(name = "cloudcache")]
#[command(about = "Cloud-backed local block cache prototype")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Create {
        #[arg(long)]
        volume_id: String,
        #[arg(long)]
        size: String,
        #[arg(long)]
        chunk_size: String,
        #[arg(long)]
        bucket: Option<String>,
        #[arg(long)]
        prefix: Option<String>,
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        #[arg(long)]
        remote_dir: Option<PathBuf>,
        #[arg(long, default_value = "local_mock", value_parser = ["local_mock", "s3"])]
        cloud_provider: String,
        #[arg(long)]
        region: Option<String>,
        #[arg(long)]
        endpoint_url: Option<String>,
        #[arg(long, default_value = "200G")]
        cache_size: String,
    },
    Attach {
        volume_id: String,
        #[arg(long, default_value = "/dev/nbd0")]
        device: String,
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
    Detach {
        volume_id: Option<String>,
        #[arg(long)]
        device: Option<String>,
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
    Status {
        volume_id: String,
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
    Sync {
        volume_id: String,
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
    Fsck {
        volume_id: String,
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
    List,
}

pub fn run(args: Vec<String>) -> std::result::Result<(), CliError> {
    let cli = Cli::try_parse_from(args).map_err(CliError::Clap)?;
    match cli.command {
        Commands::Create {
            volume_id,
            size,
            chunk_size,
            bucket,
            prefix,
            cache_dir,
            remote_dir,
            cloud_provider,
            region,
            endpoint_url,
            cache_size,
        } => create(
            volume_id,
            size,
            chunk_size,
            bucket,
            prefix,
            cache_dir,
            remote_dir,
            cloud_provider,
            region,
            endpoint_url,
            cache_size,
        )?,
        Commands::Attach {
            volume_id,
            device,
            cache_dir,
        } => attach(&volume_id, &device, cache_dir.as_deref())?,
        Commands::Detach {
            volume_id,
            device,
            cache_dir,
        } => detach(
            volume_id.as_deref(),
            device.as_deref(),
            cache_dir.as_deref(),
        )?,
        Commands::Status {
            volume_id,
            cache_dir,
        } => status(&volume_id, cache_dir.as_deref())?,
        Commands::Sync {
            volume_id,
            cache_dir,
        } => sync(&volume_id, cache_dir.as_deref())?,
        Commands::Fsck {
            volume_id,
            cache_dir,
        } => fsck(&volume_id, cache_dir.as_deref())?,
        Commands::List => list()?,
    };
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn create(
    volume_id: String,
    size: String,
    chunk_size: String,
    bucket: Option<String>,
    prefix: Option<String>,
    cache_dir: Option<PathBuf>,
    remote_dir: Option<PathBuf>,
    cloud_provider: String,
    region: Option<String>,
    endpoint_url: Option<String>,
    cache_size: String,
) -> Result<()> {
    validate_volume_id(&volume_id)?;
    let size = parse_size(&size)?;
    let chunk_size = parse_size(&chunk_size)?;
    let cache_max = parse_size(&cache_size)?;
    validate_volume_layout(size, chunk_size, cache_max)?;

    let cache_dir =
        absolute_path(cache_dir.unwrap_or_else(|| default_volume_cache_dir(&volume_id)))?;
    let remote_dir = match (&cloud_provider[..], remote_dir) {
        ("local_mock", Some(remote_dir)) => absolute_path(remote_dir)?,
        ("local_mock", None) => cache_dir.join("remote_mock"),
        ("s3", Some(remote_dir)) => absolute_path(remote_dir)?,
        ("s3", None) => PathBuf::new(),
        _ => unreachable!("clap validates cloud_provider"),
    };
    let bucket = match cloud_provider.as_str() {
        "local_mock" => bucket.unwrap_or_else(|| "local-mock".to_string()),
        "s3" => bucket.ok_or_else(|| {
            CloudError::InvalidArgument("S3 backend requires --bucket".to_string())
        })?,
        _ => unreachable!("clap validates cloud_provider"),
    };
    let prefix = prefix.unwrap_or_else(|| format!("cloudcache/volumes/{volume_id}"));

    let cloud_manifest = CloudManifest::new(volume_id.clone(), size, chunk_size);
    let local_manifest = LocalManifest::new(
        volume_id.clone(),
        cache_dir.clone(),
        crate::volume::manifest::CloudConfig {
            provider: cloud_provider,
            bucket,
            prefix,
            remote_dir,
            region,
            endpoint_url,
        },
        cache_max,
    );
    CacheEngine::create(cloud_manifest, local_manifest)?;
    register_volume(&volume_id, &cache_dir)?;
    println!("created volume {volume_id}");
    println!("cache: {}", cache_dir.display());
    Ok(())
}

fn attach(volume_id: &str, device_path: &str, cache_dir: Option<&Path>) -> Result<()> {
    let cache_dir = resolve_cache_dir(volume_id, cache_dir)?;
    let engine = CacheEngine::open(&cache_dir)?;
    atomic_write(
        &cache_dir.join("attached_device"),
        format!("{device_path}\n").as_bytes(),
    )?;
    println!("attaching {volume_id} to {device_path}; server runs in foreground");
    device::run_nbd(device_path, engine)
}

fn detach(volume_id: Option<&str>, device: Option<&str>, cache_dir: Option<&Path>) -> Result<()> {
    let device_path = if let Some(device) = device {
        device.to_string()
    } else if let Some(volume_id) = volume_id {
        let cache_dir = resolve_cache_dir(volume_id, cache_dir)?;
        fs::read_to_string(cache_dir.join("attached_device"))
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|_| "/dev/nbd0".to_string())
    } else {
        return Err(CloudError::InvalidArgument(
            "detach requires a volume id or --device".to_string(),
        ));
    };
    device::disconnect_nbd(&device_path)
}

fn status(volume_id: &str, cache_dir: Option<&Path>) -> Result<()> {
    let cache_dir = resolve_cache_dir(volume_id, cache_dir)?;
    let engine = CacheEngine::open(&cache_dir)?;
    print_status(&engine.status()?);
    Ok(())
}

fn sync(volume_id: &str, cache_dir: Option<&Path>) -> Result<()> {
    let cache_dir = resolve_cache_dir(volume_id, cache_dir)?;
    let mut engine = CacheEngine::open(&cache_dir)?;
    let uploaded = engine.sync_dirty()?;
    println!("uploaded {}", human_size(uploaded));
    Ok(())
}

fn fsck(volume_id: &str, cache_dir: Option<&Path>) -> Result<()> {
    let cache_dir = resolve_cache_dir(volume_id, cache_dir)?;
    let mut engine = CacheEngine::open(&cache_dir)?;
    let findings = engine.fsck()?;
    if findings.is_empty() {
        println!("fsck: ok");
    } else {
        for finding in findings {
            println!("fsck: {finding}");
        }
    }
    Ok(())
}

fn list() -> Result<()> {
    let registry = read_registry()?;
    if registry.is_empty() {
        println!("no volumes registered");
        return Ok(());
    }
    for (volume_id, cache_dir) in registry {
        println!("{volume_id}\t{}", cache_dir.display());
    }
    Ok(())
}

fn print_status(status: &VolumeStatus) {
    println!("Volume: {}", status.volume_id);
    println!("Size: {}", human_size(status.volume_size_bytes));
    println!("Chunk size: {}", human_size(status.chunk_size_bytes));
    println!("Cloud backend: {}", status.cloud_backend);
    println!("Cache:");
    println!(
        "  Used: {} / {}",
        human_size(status.metadata.cached_bytes),
        human_size(status.cache_max_bytes)
    );
    println!("  Clean: {}", human_size(status.metadata.clean_bytes));
    println!("  Dirty: {}", human_size(status.metadata.dirty_bytes));
    println!("  Missing chunks: {}", status.metadata.missing_chunks);
    println!("  Cached chunks: {}", status.metadata.cached_chunks);
    println!("I/O:");
    println!("  Flush semantics: local cache + journal durable");
    println!(
        "  Dirty upload backlog: {}",
        human_size(status.metadata.dirty_bytes)
    );
}

fn resolve_cache_dir(volume_id: &str, explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path.to_path_buf());
    }
    if let Some(path) = read_registry()?.get(volume_id) {
        return Ok(path.clone());
    }
    let default = default_volume_cache_dir(volume_id);
    if default.exists() {
        return Ok(default);
    }
    let cwd_default = PathBuf::from(".cloudcache").join("volumes").join(volume_id);
    if cwd_default.exists() {
        return Ok(cwd_default);
    }
    let var_default = PathBuf::from("/var/lib/cloudcache/volumes").join(volume_id);
    if var_default.exists() {
        return Ok(var_default);
    }
    Err(CloudError::NotFound(format!(
        "volume '{volume_id}' is not registered; pass --cache-dir"
    )))
}

fn register_volume(volume_id: &str, cache_dir: &Path) -> Result<()> {
    let registry = registry_path();
    if let Some(parent) = registry.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut map = read_registry()?;
    map.insert(volume_id.to_string(), cache_dir.to_path_buf());
    let mut rows: Vec<_> = map.into_iter().collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let mut bytes = Vec::new();
    for (id, path) in rows {
        bytes.extend_from_slice(format!("{id}\t{}\n", path.display()).as_bytes());
    }
    atomic_write(&registry, &bytes)?;
    Ok(())
}

fn read_registry() -> Result<HashMap<String, PathBuf>> {
    let path = registry_path();
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let text = fs::read_to_string(&path)?;
    let mut map = HashMap::new();
    for (line_number, line) in text.lines().enumerate() {
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let Some((id, path)) = line.split_once('\t') else {
            return Err(CloudError::Corrupt(format!(
                "invalid registry line {} in {}",
                line_number + 1,
                path.display()
            )));
        };
        map.insert(id.to_string(), PathBuf::from(path));
    }
    Ok(map)
}

fn validate_volume_id(volume_id: &str) -> Result<()> {
    if volume_id.is_empty() {
        return Err(CloudError::InvalidArgument(
            "volume-id must not be empty".to_string(),
        ));
    }
    if volume_id.len() > 128 {
        return Err(CloudError::InvalidArgument(
            "volume-id must be at most 128 bytes".to_string(),
        ));
    }
    if volume_id == "." || volume_id == ".." {
        return Err(CloudError::InvalidArgument(
            "volume-id must not be '.' or '..'".to_string(),
        ));
    }
    if !volume_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(CloudError::InvalidArgument(
            "volume-id may only contain ASCII letters, digits, '.', '_' and '-'".to_string(),
        ));
    }
    Ok(())
}

fn validate_volume_layout(size: u64, chunk_size: u64, cache_max: u64) -> Result<()> {
    if size == 0 || chunk_size == 0 || cache_max == 0 {
        return Err(CloudError::InvalidArgument(
            "size, chunk-size and cache-size must be non-zero".to_string(),
        ));
    }
    if chunk_size > size {
        return Err(CloudError::InvalidArgument(
            "chunk-size must not exceed volume size".to_string(),
        ));
    }
    for (name, value) in [
        ("size", size),
        ("chunk-size", chunk_size),
        ("cache-size", cache_max),
    ] {
        if !value.is_multiple_of(DEFAULT_SECTOR_SIZE_BYTES) {
            return Err(CloudError::InvalidArgument(format!(
                "{name} must be a multiple of {DEFAULT_SECTOR_SIZE_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

fn absolute_path(path: PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}
