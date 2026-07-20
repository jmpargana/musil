use crate::protocol::fetch::response::partition_response::PartitionResponse;

#[derive(Debug)]
pub struct TopicResponse {
    pub topic: String,
    pub partitions: Vec<PartitionResponse>,
}

impl TopicResponse {
    pub fn get_size(&self) -> u32 {
        2 + self.topic.len() as u32 + 4 + self.partitions.iter().map(|p| p.get_size()).sum::<u32>()
    }
}
