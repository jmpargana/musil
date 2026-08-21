use std::{collections::HashMap, time::Duration};

use proto::record::Record;

use crate::protocol::produce::{
    acks::Acks,
    request::{produce_partition::ProducePartition, produce_topic::ProduceTopic},
};

#[derive(Debug)]
pub struct ProduceRequest {
    pub transactional_id: u64,
    pub acks: Acks,
    pub timeout: Duration,
    pub topics: Vec<ProduceTopic>,
}

impl From<HashMap<String, HashMap<u16, Vec<Record>>>> for ProduceRequest {
    fn from(value: HashMap<String, HashMap<u16, Vec<Record>>>) -> Self {
        let topics = value
            .into_iter()
            .map(|(topic, partitions)| ProduceTopic {
                topic,
                partitions: partitions
                    .into_iter()
                    .map(|(index, records)| ProducePartition {
                        index,
                        records: records.into(),
                    })
                    .collect(),
            })
            .collect();

        // FIXME: hardcoding this for now
        ProduceRequest {
            transactional_id: 0,
            acks: Acks::Leader,
            timeout: Duration::from_secs(10),
            topics,
        }
    }
}
