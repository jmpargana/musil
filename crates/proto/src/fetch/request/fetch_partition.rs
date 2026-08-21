use std::u32;

#[derive(Debug)]
pub struct FetchPartition {
    pub partition: u32,
    pub fetch_offset: u64,
    pub partition_max_bytes: u32,
    pub high_watermark: u64,
}

impl FetchPartition {
    // TODO: Hardcoded to work with Raft. The default values might not work with other flows.
    pub fn from(fetch_offset: u64) -> Self {
        Self {
            partition: 0,
            fetch_offset,
            partition_max_bytes: u32::MAX,
            high_watermark: 0,
        }
    }
}
