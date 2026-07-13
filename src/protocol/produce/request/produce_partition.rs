use crate::storage::record_batch::RecordBatch;

// OR: batch: Bytes which already is a reference to the underlying bytes
pub struct ProducePartition {
    pub index: u16,
    pub records: RecordBatch,
    // pub records: Bytes,
}
