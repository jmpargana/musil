use std::time::Duration;

use derive_builder::Builder;

use crate::message::{ack::Ack, consumer::FetchRequest, produce::ProduceTopic};

pub enum MessageBody {
    Produce(ProduceRequest),
    ProduceResponse,
    Fetch(FetchRequest),
    FetchResponse,
}

pub struct ProduceRequest {
    transactional_id: u64,
    acks: Ack,
    timeout: Duration,
    topics: Vec<ProduceTopic>,
}
