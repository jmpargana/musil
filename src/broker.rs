use std::{collections::HashMap, io, time::Instant};

use crate::{
    message::{
        Message,
        body::{MessageBody, ProduceRequest},
        consumer::{FetchResponse, TopicResponse},
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
        self.partitions
            .get(&TopicPartition {
                topic_id: topic.to_owned(),
                partition_id: partition,
            })
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown partition"))
    }

    // TODO: maybe internal handler returns body, this wraps in Message?
    pub async fn handle(&self, req: Message) -> io::Result<Message> {
        // api key redundant since body already has format
        match &req.body {
            MessageBody::Fetch(body) => self.handle_fetch(&req, &body).await,
            MessageBody::Produce(body) => self.handle_produce(&body).await,
            _ => panic!("unsupported"),
        }
    }

    // TODO: refactor to only respond to body, or extract body only.
    async fn handle_fetch(
        &self,
        // TODO: maybe this doesn't have to be referrence, instead move, so that no clone is needed.
        req: &Message,
        body: &crate::message::consumer::FetchRequest,
    ) -> io::Result<Message> {
        let now = Instant::now();
        let mut topic_responses = Vec::new();

        for t in &body.topics {
            let mut part_responses = Vec::new();
            for p in &t.partitions {
                let partition = self.partition(&t.topic, p.partition as u16)?;
                let part_res = partition.fetch(&p, body.replica_id).await;
                part_responses.push(part_res);
            }

            topic_responses.push(TopicResponse {
                topic: t.topic.clone(),
                partitions: part_responses,
            });
        }

        let fetch_response = FetchResponse {
            throttle_time_ms: now.elapsed().as_millis() as u32, // TODO: how to cast to u32?,
            responses: topic_responses,
        };

        // TODO: is it the same header?
        let header = req.header.clone();
        let size = fetch_response.get_size() + header.get_size();

        let body = MessageBody::FetchResponse(fetch_response);

        let msg = Message { size, header, body };

        Ok(msg)
    }

    async fn handle_produce(&self, body: &ProduceRequest) -> io::Result<Message> {
        todo!()
    }
}
