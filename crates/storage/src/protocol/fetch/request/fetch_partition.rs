#[derive(Debug)]
pub struct FetchPartition {
    pub partition: u32,
    pub fetch_offset: u64,
    pub partition_max_bytes: u32,
    pub high_watermark: u64,
}
