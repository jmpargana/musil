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
            + self.partition_responses.iter().map(|it| it.get_size()).sum::<u32>()
    }
}
