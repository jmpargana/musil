use crate::protocol::fetch::response::partition_response::PartitionResponse;

pub struct TopicResponse {
    pub topic: String,
    pub partitions: Vec<PartitionResponse>,
}

impl TopicResponse {
    pub fn get_size(&self) -> u32 {
        self.topic.len() as u32 + self.partitions.iter().map(|p| p.get_size()).sum::<u32>()
    }
}
