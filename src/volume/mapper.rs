use crate::error::{CloudError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkSpan {
    pub chunk_id: u64,
    pub offset_in_chunk: u64,
    pub request_offset: usize,
    pub len: usize,
}

pub fn split(offset: u64, len: usize, chunk_size: u64, volume_size: u64) -> Result<Vec<ChunkSpan>> {
    if chunk_size == 0 {
        return Err(CloudError::InvalidArgument(
            "chunk size must be non-zero".to_string(),
        ));
    }
    let end = offset
        .checked_add(len as u64)
        .ok_or_else(|| CloudError::InvalidArgument("request range overflows u64".to_string()))?;
    if end > volume_size {
        return Err(CloudError::InvalidArgument(format!(
            "request range {offset}..{end} exceeds volume size {volume_size}"
        )));
    }

    let mut spans = Vec::new();
    let mut remaining = len;
    let mut current_offset = offset;
    let mut request_offset = 0usize;

    while remaining > 0 {
        let chunk_id = current_offset / chunk_size;
        let offset_in_chunk = current_offset % chunk_size;
        let available = (chunk_size - offset_in_chunk) as usize;
        let take = available.min(remaining);
        spans.push(ChunkSpan {
            chunk_id,
            offset_in_chunk,
            request_offset,
            len: take,
        });
        remaining -= take;
        request_offset += take;
        current_offset += take as u64;
    }

    Ok(spans)
}

#[cfg(test)]
mod tests {
    use super::{ChunkSpan, split};

    #[test]
    fn splits_cross_chunk_request() {
        assert_eq!(
            split(16_777_200, 8192, 16 * 1024 * 1024, 64 * 1024 * 1024).unwrap(),
            vec![
                ChunkSpan {
                    chunk_id: 0,
                    offset_in_chunk: 16_777_200,
                    request_offset: 0,
                    len: 16,
                },
                ChunkSpan {
                    chunk_id: 1,
                    offset_in_chunk: 0,
                    request_offset: 16,
                    len: 8176,
                }
            ]
        );
    }
}
