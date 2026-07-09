use std::{
    self, Error, collections::HashMap, io},
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
    // FIXME: use partitionhandle directly.
    partitions: HashMap<TopicPartition, Partition>,
}

impl Broker {
    pub fn partition(&self, topic: &str, partition: u16) -> io::Result<&Partition> {
        self.partitions.get(&TopicPartition { topic_id: topic.to_owned(), partition_id: partition })    
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "unknown partition")
            })
    }

    // TODO: maybe internal handler returns body, this wraps in Message?
    pub async fn handle(&self, req: Message) -> io::Result<Message> {
        // api key redundant since body already has format
        match req.body {
            MessageBody::Fetch(body) => self.handle_fetch(body),
            MessageBody::Produce(body)  => self.handle_produce(body),
            _ => panic!("unsupported"),
        }
        // let res = match req.header.api_key {
        //     crate::message::header::MessageApiKey::Produce => {
        //         if let MessageBody::Produce {
        //             transactional_id: _,
        //             acks: _,
        //             timeout: _,
        //             topics,
        //         } = req.body
        //         {
        //             for topic in topics {
        //                 for partition in topic.partitions {
        //                     let p = self
        //                         .partitions
        //                         .get(&TopicPartition {
        //                             topic_id: topic.topic.to_string(),
        //                             partition_id: partition.partition_id,
        //                         })
        //                         // TODO: handle missing partition
        //                         .unwrap();

        //                     // TODO: gather response
        //                     p.produce(partition.batch).await;
        //                 }
        //             }
        //         }
        //         let header = MessageHeaderBuilder::default().build().unwrap();
        //         Message {
        //             size: 0,
        //             header,
        //             body: MessageBody::FetchResponse,
        //         }
        //     }
        //     crate::message::header::MessageApiKey::Fetch => {
        //         if let MessageBody::Fetch(payload) = req.body {
        //             for topic in payload.topics {
        //                 for partition in topic.partitions {
        //                     let p = self
        //                         .partitions
        //                         .get(&TopicPartition {
        //                             topic_id: topic.topic.to_string(),
        //                             partition_id: partition.partition as u16,
        //                         })
        //                         .unwrap();

        //                     p.fetch(partition, payload.replica_id);
        //                 }
        //             }
        //         };
        //         // TODO: combine payloads
        //         Message {
        //             size: (),
        //             header: (),
        //             body: (),
        //         }
        //     }
        // };
        Ok(res)
    }

    fn handle_fetch(&self, body: crate::message::consumer::FetchRequest) -> io::Result<Message> {
        for t in body.topics {
            for p in t.partitions {
                let partition = self.partition(&t.topic, p.partition as u16)?;
                partition.fetch(p, body.replica_id);
            }
        }
        // FIXME: actually return response
        Ok(())
    }

    fn handle_produce(&self, body: _) -> io::Result<Message> {
        todo!()
    }
}
