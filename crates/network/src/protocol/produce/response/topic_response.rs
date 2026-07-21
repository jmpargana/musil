use std::fmt::{Display, write};

use crate::protocol::produce::response::partition_response::ProducePartitionResponse;

#[derive(Debug)]
pub struct ProduceTopicResponse {
    pub topic: String,
    pub partition_responses: Vec<ProducePartitionResponse>,
}

impl ProduceTopicResponse {
    pub(crate) fn get_size(&self) -> u32 {
        2 + self.topic.len() as u32
            + 4
            + self
                .partition_responses
                .iter()
                .map(|it| it.get_size())
                .sum::<u32>()
    }
}

impl Display for ProduceTopicResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "topic: {}, partitions: [", self.topic,)?;

        for (i, partition) in self.partition_responses.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{partition}")?;
        }

        write!(f, "]")
    }
}
