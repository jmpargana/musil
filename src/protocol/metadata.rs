use crate::protocol::error_codes::ErrorCode;

#[derive(Debug)]
pub struct MetadataRequest {
    pub topics: Vec<String>,
    pub allow_auto_topic_creation: bool,
}

impl MetadataRequest {
    pub fn get_size(&self) -> u32 {
        4 + self.topics.iter().map(|t| 2 + t.len() as u32).sum::<u32>() + 1
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

// TODO: refactor these records somewhere else
#[derive(Debug)]
pub struct CreateTopicRequest {
    pub topics: Vec<TopicRequest>,
    pub timeout_ms: u32,
    pub validate_only: bool,
}

impl CreateTopicRequest {
    pub fn get_size(&self) -> u32 {
        // topics_count(2) + topics + timeout_ms(4) + validate_only(1)
        2 + self.topics.iter().map(|t| t.get_size()).sum::<u32>() + 4 + 1
    }
}

#[derive(Debug)]
pub struct TopicRequest {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: u16,
    pub assignments: Vec<TopicPartitonAssignment>,
}

impl TopicRequest {
    fn get_size(&self) -> u32 {
        // name_len(2) + name + num_partitions(4) + replication_factor(2) + assignments_count(2) + assignments
        2 + self.name.len() as u32 + 4 + 2 + 2 + self.assignments.iter().map(|a| a.get_size()).sum::<u32>()
    }
}

#[derive(Debug)]
pub struct TopicPartitonAssignment {
    pub partition_index: i32,
    pub broker_ids: i32,
}

impl TopicPartitonAssignment {
    fn get_size(&self) -> u32 {
        // partition_index(4) + broker_ids(4)
        4 + 4
    }
}

#[derive(Debug)]
pub struct CreateTopicResponse {
    pub throttle_time_ms: u32,
    pub topics: Vec<TopicResponse>,
}

impl CreateTopicResponse {
    pub fn get_size(&self) -> u32 {
        // throttle_time_ms(4) + topics_count(2) + topics
        4 + 2 + self.topics.iter().map(|t| t.get_size()).sum::<u32>()
    }
}

#[derive(Debug)]
pub struct TopicResponse {
    pub name: String,
    pub error_code: ErrorCode,
    pub error_message: String,
    pub num_partitions: i32,
    pub replication_factor: u16,
}

impl TopicResponse {
    fn get_size(&self) -> u32 {
        // name_len(2) + name + error_code(2) + error_message_len(2) + error_message + num_partitions(4) + replication_factor(2)
        2 + self.name.len() as u32 + 2 + 2 + self.error_message.len() as u32 + 4 + 2
    }
}

#[derive(Debug)]
pub struct TopicRecord {
    pub name: String,
}

type BrokerId = i32;

#[derive(Debug)]
pub struct PartitionRecord {
    pub partition_id: i32,
    // FIXME: actually this should be an uuid, but I'm sticking to String everywhere
    pub topic_id: String,

    pub replicas: Vec<BrokerId>,
    pub isr: Vec<BrokerId>,

    pub leader: BrokerId,
}
