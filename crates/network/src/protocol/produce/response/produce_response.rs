use crate::protocol::produce::response::topic_response::ProduceTopicResponse;

#[derive(Debug)]
pub struct ProduceResponse {
    pub throttle_time_ms: u32,
    pub responses: Vec<ProduceTopicResponse>,
}

impl ProduceResponse {
    pub fn get_size(&self) -> u32 {
        4 + 4 + self.responses.iter().map(|r| r.get_size()).sum::<u32>()
    }
}
