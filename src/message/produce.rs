use bytes::Bytes;

pub struct ProduceTopic {
    pub topic: String,
    pub partitions: Vec<ProducePartition>,
}

// OR: batch: Bytes which already is a reference to the underlying bytes
pub struct ProducePartition {
    pub partition_id: u16,
    pub batch: Bytes,
}
