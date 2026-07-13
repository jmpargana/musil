use crate::protocol::produce::response::partition_response::ProducePartitionResponse;

pub struct ProduceTopicResponse {
    pub topic: String,
    pub partition_responses: Vec<ProducePartitionResponse>,
}
impl ProduceTopicResponse {
    pub(crate) fn get_size(&self) -> u32 {
        self.topic.len() as u32
            + self
                .partition_responses
                .iter()
                .map(|it| it.get_size())
                .sum::<u32>()
    }
}
