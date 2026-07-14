use crate::protocol::error_codes::ErrorCode;

pub struct MetadataRequest {
    pub topics: Vec<String>,
    pub allow_auto_topic_creation: bool,
}

pub struct MetadataResponse {
    pub throttle_time_ms: u32,
    pub brokers: Vec<BrokerMetadata>,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
    pub error_code: ErrorCode,
}

impl MetadataResponse {
    pub fn get_size(&self) -> u32 {
        4 + 4
            + 2
        // include size of arrays
            + 4
            + 4
            + self.brokers.iter().map(|b| b.get_size()).sum::<u32>()
            + self.topics.iter().map(|t| t.get_size()).sum::<u32>()
    }
}

pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}
impl BrokerMetadata {
    fn get_size(&self) -> u32 {
        // include size of string length
        4 + 4 + 2 + self.host.len() as u32
    }
}

pub struct TopicMetadata {
    pub error_code: ErrorCode,
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
}
impl TopicMetadata {
    fn get_size(&self) -> u32 {
        2 + 2
            + self.name.len() as u32
            + 4
            + self.partitions.iter().map(|p| p.get_size()).sum::<u32>()
    }
}

pub struct PartitionMetadata {
    pub error_code: ErrorCode,
    pub partition_index: i32,
    pub leader_id: i32,
    pub replica_nodes: u32,
    pub isr_nodes: u32,
    pub offline_replicas: u32,
}
impl PartitionMetadata {
    fn get_size(&self) -> u32 {
        2 + 4 + 4 + 4 + 4 + 4
    }
}
