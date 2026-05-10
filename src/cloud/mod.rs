pub mod local_mock;

use crate::error::Result;

pub trait CloudBackend {
    fn fetch_chunk(&self, chunk_id: u64, expected_size: u64) -> Result<Option<Vec<u8>>>;
    fn upload_chunk(&self, chunk_id: u64, generation: u64, data: &[u8]) -> Result<String>;
}
