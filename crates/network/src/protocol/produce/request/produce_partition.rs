use proto::record_batch::RecordBatch;

#[derive(Debug)]
pub struct ProducePartition {
    pub index: u16,
    pub records: RecordBatch,
}
