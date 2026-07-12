use crate::protocol::produce::response::partition_response::ProducePartitionResponse;

pub struct ProduceTopicResponse {
    pub topic: String,
    pub partition_responses: Vec<ProducePartitionResponse>,
}
