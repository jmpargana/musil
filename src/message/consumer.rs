use crate::{batch::Batch, record::Record};

pub struct FetchRequest {
    pub replica_id: i32,
    // TODO: ignoring these fields for now
    // pub max_wait_ms: u32,
    // pub min_bytes: u32,
    pub max_bytes: u32,
    pub topics: Vec<FetchTopic>,
}

pub struct FetchTopic {
    pub topic: String,
    pub partitions: Vec<FetchPartition>,
}

pub struct FetchPartition {
    pub partition: u32,
    pub fetch_offset: u64,
    // This field is only used when the request is sent by the follower.
    // TODO: need to figure out how to encode.
    pub log_start_offset: Option<u64>,
    // This limit mayb not be honored.
    pub partition_max_bytes: u32,
    // This field is only used when the request is sent by the follower.
    // TODO: need to figure out how to encode.
    pub high_watermark: Option<u64>,
}

pub struct FetchResponse {
    pub throttle_time_ms: u32,
    pub responses: Vec<TopicResponse>,
}

impl FetchResponse {
    pub fn get_size(&self) -> u32 {
        4 + self.responses.iter().map(|r| r.get_size()).sum()
    }
}

pub struct TopicResponse {
    pub topic: String,
    pub partitions: Vec<PartitionResponse>,
}

impl TopicResponse {
    pub fn get_size(&self) -> u32 {
        self.topic.len() + self.partitions.iter().map(|p| p.get_size()).sum()
    }
}

// TODO: include transactional fields.
pub struct PartitionResponse {
    pub partition_index: u32,
    // TODO: introduce error enum with conversion
    pub error_code: u16,
    pub high_watermark: u64,
    pub log_start_offset: u64,
    pub records: Vec<Batch>,
    // TODO: include leader.
}

impl PartitionResponse {
    pub fn get_size(&self) -> u64 {
        // each field plus records
        4 + 2 + 8 + 8 + self.records.iter().map(|b| b.get_size()).sum()
    }
}
