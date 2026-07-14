use crate::protocol::produce::response::partition_response::ProducePartitionResponse;

#[derive(Debug)]
pub struct ProduceTopicResponse {
    pub topic: String,
    pub partition_responses: Vec<ProducePartitionResponse>,
}
impl ProduceTopicResponse {
    pub(crate) fn get_size(&self) -> u32 {
        // topic_len_prefix(2) + topic_bytes + partition_responses_count(4) + each partition
        2 + self.topic.len() as u32
            + 4
            + self
                .partition_responses
                .iter()
                .map(|it| it.get_size())
                .sum::<u32>()
    }
}
