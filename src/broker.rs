use std::{
    collections::HashMap,
    io::{self, Error},
    sync::Arc,
};

use crate::{
    message::{
        Message,
        body::MessageBody,
        header::{MessageHeader, MessageHeaderBuilder},
    },
    partition::Partition,
    topic::TopicPartition,
};

pub struct Broker {
    // TODO: needs to be behind Arc in case topics and partitions are dynamic, otherwise broker restart is needed
    partitions: HashMap<TopicPartition, Partition>,
}

impl Broker {
    pub async fn handle(&self, req: Message) -> io::Result<Message> {
        let res = match req.header.api_key {
            crate::message::header::MessageApiKey::Produce => {
                if let MessageBody::Produce {
                    transactional_id: _,
                    acks: _,
                    timeout: _,
                    topics,
                } = req.body
                {
                    for topic in topics {
                        for partition in topic.partitions {
                            let p = self
                                .partitions
                                .get(&TopicPartition {
                                    topic_id: topic.topic.to_string(),
                                    partition_id: partition.partition_id,
                                })
                                // TODO: handle missing partition
                                .unwrap();

                            // TODO: gather response
                            p.produce(partition.batch).await;
                        }
                    }
                }
                let header = MessageHeaderBuilder::default().build().unwrap();
                Message {
                    size: 0,
                    header,
                    body: MessageBody::FetchResponse,
                }
            }
            crate::message::header::MessageApiKey::Fetch => {}
        };
        Ok(res)
    }
}
