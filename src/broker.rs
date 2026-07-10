use std::{collections::HashMap, io, time::Instant};

use crate::{
    partition::Partition,
    protocol::{
        Frame,
        body::FrameBody,
        fetch::response::{
            fetch_response::FetchResponse, topic_response::TopicResponse,
        },
        produce::request::produce_request::ProduceRequest,
    },
    topic::TopicPartition,
};

pub struct Broker {
    // TODO: needs to be behind Arc in case topics and partitions are dynamic, otherwise broker restart is needed
    // FIXME: use PartitionHandle directly.
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

    // TODO: maybe internal handler returns body, this wraps in Frame?
    pub async fn handle(&self, req: Frame) -> io::Result<Frame> {
        match &req.body {
            FrameBody::Fetch(body) => self.handle_fetch(&req, body).await,
            FrameBody::Produce(body) => self.handle_produce(body).await,
            _ => panic!("unsupported"),
        }
    }

    async fn handle_fetch(
        &self,
        req: &Frame,
        body: &crate::protocol::fetch::request::fetch_request::FetchRequest,
    ) -> io::Result<Frame> {
        let now = Instant::now();
        let mut topic_responses = Vec::new();

        for t in &body.topics {
            let mut part_responses = Vec::new();
            for p in &t.partitions {
                let partition = self.partition(&t.topic, p.partition as u16)?;
                let part_res = partition.fetch(p, body.replica_id).await;
                part_responses.push(part_res);
            }

            topic_responses.push(TopicResponse {
                topic: t.topic.clone(),
                partitions: part_responses,
            });
        }

        let fetch_response = FetchResponse {
            throttle_time_ms: now.elapsed().as_millis() as u32,
            responses: topic_responses,
        };

        let header = req.header.clone();
        let size = fetch_response.get_size() + header.get_size();

        let body = FrameBody::FetchResponse(fetch_response);

        let frame = Frame { size, header, body };

        Ok(frame)
    }

    async fn handle_produce(&self, _body: &ProduceRequest) -> io::Result<Frame> {
        todo!()
    }
}
