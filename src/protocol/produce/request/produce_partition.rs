use bytes::Bytes;

// OR: batch: Bytes which already is a reference to the underlying bytes
pub struct ProducePartition {
    pub partition_id: u16,
    pub batch: Bytes,
}
