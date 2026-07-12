use bytes::Bytes;

// OR: batch: Bytes which already is a reference to the underlying bytes
pub struct ProducePartition {
    pub index: u16,
    pub records: Bytes,
}
