use core::fmt;

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

impl fmt::Display for TopicResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "topic: {}", self.topic)?;
        for p in self.partitions.iter() {
            writeln!(f, "\t\t{}", p)?;
        }
        Ok(())
    }
}
