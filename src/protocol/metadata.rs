use crate::protocol::error_codes::ErrorCode;

pub struct MetadataRequest {
    pub topics: Vec<String>,
    pub allow_auto_topic_creation: bool,
}

pub struct MetadataResponse {
    pub throttle_time_ms: u32,
    pub brokers: Vec<BrokerConfig>,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
    pub error_code: ErrorCode,
}

pub struct BrokerConfig {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

pub struct TopicMetadata {
    pub error_code: ErrorCode,
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
}

pub struct PartitionMetadata {
    pub error_code: ErrorCode,
    pub partition_index: i32,
    pub leader_id: i32,
    pub replica_nodes: u32,
    pub isr_nodes: u32,
    pub offline_replicas: u32,
}
