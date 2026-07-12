use crate::protocol::produce::response::topic_response::ProduceTopicResponse;

pub struct ProduceResponse {
    pub throttle_time_ms: u32,
    pub responses: Vec<ProduceTopicResponse>,
}
