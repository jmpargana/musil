use crate::protocol::error_codes::ErrorCode;
use crate::storage::record_batch::RecordBatch;

#[derive(Debug)]
pub struct PartitionResponse {
    pub partition_index: u32,
    pub error_code: ErrorCode,
    pub high_watermark: u64,
    pub log_start_offset: u64,
    pub records: Vec<RecordBatch>,
}

impl PartitionResponse {
    pub fn error(partition_index: u32, error_code: ErrorCode) -> Self {
        Self {
            partition_index,
            error_code,
            high_watermark: 0,
            log_start_offset: 0,
            records: vec![],
        }
    }

    pub fn get_size(&self) -> u32 {
        4 + 2 + 8 + 8 + 4 + self.records.iter().map(|b| b.get_size()).sum::<u32>()
    }
}
