use std::time::Duration;

use derive_builder::Builder;

use crate::message::{ack::Ack, produce::ProduceTopic};

pub enum MessageBody {
    Produce {
        transactional_id: u64,
        acks: Ack,
        timeout: Duration,
        topics: Vec<ProduceTopic>,
    },
    ProduceResponse,
    Fetch,
    FetchResponse,
}
