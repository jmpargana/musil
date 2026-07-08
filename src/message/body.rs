use std::time::Duration;

use crate::message::{ack::Ack, produce::ProduceTopic};

pub enum MessageBody {
    Produce {
        transactional_id: u64,
        acks: Ack,
        timeout: Duration,
        topics: Vec<ProduceTopic>,
    },
    FetchResponse,
}
