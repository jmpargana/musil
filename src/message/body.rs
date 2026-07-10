use std::time::Duration;

use crate::message::{
    ack::Ack,
    consumer::{FetchRequest, FetchResponse},
    produce::ProduceTopic,
};

pub enum MessageBody {
    Produce(ProduceRequest),
    ProduceResponse,
    Fetch(FetchRequest),
    FetchResponse(FetchResponse),
}

pub struct ProduceRequest {
    pub transactional_id: u64,
    pub acks: Ack,
    pub timeout: Duration,
    pub topics: Vec<ProduceTopic>,
}
