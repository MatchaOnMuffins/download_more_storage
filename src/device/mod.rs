#[cfg(target_os = "linux")]
mod nbd_linux;

#[cfg(target_os = "linux")]
pub use nbd_linux::{disconnect_nbd, run_nbd};

#[cfg(not(target_os = "linux"))]
use crate::cache::engine::CacheEngine;
#[cfg(not(target_os = "linux"))]
use crate::error::{CloudError, Result};

#[cfg(not(target_os = "linux"))]
pub fn run_nbd(_device_path: &str, _engine: CacheEngine) -> Result<()> {
    Err(CloudError::Unsupported(
        "NBD attach is only available on Linux".to_string(),
    ))
}

#[cfg(not(target_os = "linux"))]
pub fn disconnect_nbd(_device_path: &str) -> Result<()> {
    Err(CloudError::Unsupported(
        "NBD detach is only available on Linux".to_string(),
    ))
}
