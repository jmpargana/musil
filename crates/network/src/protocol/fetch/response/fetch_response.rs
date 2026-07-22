use core::fmt;

use crate::protocol::fetch::response::topic_response::TopicResponse;

#[derive(Debug)]
pub struct FetchResponse {
    pub throttle_time_ms: u32,
    pub responses: Vec<TopicResponse>,
}

impl FetchResponse {
    pub fn get_size(&self) -> u32 {
        4 + 4 + self.responses.iter().map(|r| r.get_size()).sum::<u32>()
    }
}

impl fmt::Display for FetchResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for t in self.responses.iter() {
            write!(f, "{t}")?;
        }
        Ok(())
    }
}
