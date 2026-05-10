use crate::cache::engine::{CacheEngine, VolumeStatus};
use crate::device;
use crate::error::{CloudError, Result};
use crate::util::{atomic_write, human_size, parse_size, registry_path};
use crate::volume::manifest::{CloudManifest, LocalManifest};
use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Parser)]
#[command(name = "cloudcache")]
#[command(about = "Cloud-backed local block cache prototype")]
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

pub fn run(args: Vec<String>) -> Result<()> {
    let cli =
        Cli::try_parse_from(args).map_err(|err| CloudError::InvalidArgument(err.to_string()))?;
    match cli.command {
        Commands::Create {
            volume_id,
            size,
            chunk_size,
            bucket,
            prefix,
            cache_dir,
            remote_dir,
            cache_size,
        } => create(
            volume_id, size, chunk_size, bucket, prefix, cache_dir, remote_dir, cache_size,
        ),
        Commands::Attach {
            volume_id,
            device,
            cache_dir,
        } => attach(&volume_id, &device, cache_dir.as_deref()),
        Commands::Detach {
            volume_id,
            device,
            cache_dir,
        } => detach(
            volume_id.as_deref(),
            device.as_deref(),
            cache_dir.as_deref(),
        ),
        Commands::Status {
            volume_id,
            cache_dir,
        } => status(&volume_id, cache_dir.as_deref()),
        Commands::Sync {
            volume_id,
            cache_dir,
        } => sync(&volume_id, cache_dir.as_deref()),
        Commands::Fsck {
            volume_id,
            cache_dir,
        } => fsck(&volume_id, cache_dir.as_deref()),
        Commands::List => list(),
    }
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
    cache_size: String,
) -> Result<()> {
    let size = parse_size(&size)?;
    let chunk_size = parse_size(&chunk_size)?;
    if chunk_size == 0 || size == 0 {
        return Err(CloudError::InvalidArgument(
            "size and chunk-size must be non-zero".to_string(),
        ));
    }

    let cache_dir = cache_dir.unwrap_or_else(|| {
        PathBuf::from(".cloudcache")
            .join("volumes")
            .join(&volume_id)
    });
    let remote_dir = remote_dir.unwrap_or_else(|| cache_dir.join("remote_mock"));
    let cache_max = parse_size(&cache_size)?;
    let bucket = bucket.unwrap_or_else(|| "local-mock".to_string());
    let prefix = prefix.unwrap_or_else(|| format!("cloudcache/volumes/{volume_id}"));

    let cloud_manifest = CloudManifest::new(volume_id.clone(), size, chunk_size);
    let local_manifest = LocalManifest::new(
        volume_id.clone(),
        cache_dir.clone(),
        bucket,
        prefix,
        remote_dir,
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
    println!("Remote mock: {}", status.remote_dir.display());
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
    let default = PathBuf::from(".cloudcache").join("volumes").join(volume_id);
    if default.exists() {
        return Ok(default);
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
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&registry)?;
    for (id, path) in rows {
        writeln!(file, "{id}\t{}", path.display())?;
    }
    file.sync_all()?;
    Ok(())
}

fn read_registry() -> Result<HashMap<String, PathBuf>> {
    let path = registry_path();
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let text = fs::read_to_string(path)?;
    let mut map = HashMap::new();
    for line in text.lines() {
        let Some((id, path)) = line.split_once('\t') else {
            continue;
        };
        map.insert(id.to_string(), PathBuf::from(path));
    }
    Ok(map)
}
