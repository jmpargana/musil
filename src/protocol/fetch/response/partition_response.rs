use crate::protocol::error_codes::ErrorCode;
use crate::storage::record_batch::RecordBatch;

// TODO: include transactional fields.
pub struct PartitionResponse {
    pub partition_index: u32,
    pub error_code: ErrorCode,
    pub high_watermark: u64,
    pub log_start_offset: u64,
    pub records: Vec<RecordBatch>,
    // TODO: include leader.
}

impl PartitionResponse {
    pub fn get_size(&self) -> u32 {
        // each field plus records
        4 + 2 + 8 + 8 + self.records.iter().map(|b| b.get_size()).sum::<u32>()
    }
}
