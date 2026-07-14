use std::time::Duration;

use crate::protocol::produce::{acks::Acks, request::produce_topic::ProduceTopic};

#[derive(Debug)]
pub struct ProduceRequest {
    pub transactional_id: u64,
    pub acks: Acks,
    pub timeout: Duration,
    pub topics: Vec<ProduceTopic>,
}
