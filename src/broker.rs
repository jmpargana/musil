use std::{collections::HashMap, io, sync::Arc, time::Instant};

use crate::{
    partition::handle::PartitionHandle,
    protocol::{
        Frame,
        body::FrameBody::{self},
        fetch::{
            request::fetch_request::FetchRequest,
            response::{fetch_response::FetchResponse, topic_response::TopicResponse},
        },
        produce::{
            request::produce_request::ProduceRequest,
            response::{produce_response::ProduceResponse, topic_response::ProduceTopicResponse},
        },
    },
    topic::TopicPartition,
};

pub struct Broker {
    // TODO: needs to be behind Arc in case topics and partitions are dynamic, otherwise broker restart is needed
    partitions: HashMap<TopicPartition, Arc<PartitionHandle>>,
}

impl Broker {
    pub fn new() -> Self {
        todo!()
    }

    pub fn update(&mut self) {}

    pub fn partition(&self, topic: &str, partition: u16) -> io::Result<&Arc<PartitionHandle>> {
        self.partitions
            .get(&TopicPartition {
                topic_id: topic.to_owned(),
                partition_id: partition,
            })
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown partition"))
    }

    pub async fn handle(&self, req: Frame) -> io::Result<Frame> {
        match &req.body {
            FrameBody::Fetch(_) => self.handle_fetch(req).await,
            FrameBody::Produce(_) => self.handle_produce(req).await,
            _ => panic!("unsupported"),
        }
    }

    // TODO: refactor shared flow into single method
    async fn handle_fetch(&self, req: Frame) -> io::Result<Frame> {
        let FrameBody::Fetch(body) = req.body else {
            unreachable!()
        };
        let now = Instant::now();
        let mut topic_responses = Vec::new();

        for t in body.topics {
            let mut part_responses = Vec::new();
            for p in t.partitions {
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

        Ok(Frame { size, header, body })
    }

    async fn handle_produce(&self, req: Frame) -> io::Result<Frame> {
        let FrameBody::Produce(body) = req.body else {
            unreachable!()
        };
        let now = Instant::now();
        let mut topic_responses = Vec::new();

        for t in body.topics {
            let mut part_responses = Vec::new();
            for p in t.partitions {
                let partition = self.partition(&t.topic, p.index as u16)?;
                let part_res = partition.append(p.records, body.acks).await;
                part_responses.push(part_res);
            }
            topic_responses.push(ProduceTopicResponse {
                topic: t.topic,
                partition_responses: part_responses,
            });
        }

        let header = req.header.clone();
        let produce_response = ProduceResponse {
            throttle_time_ms: now.elapsed().as_millis() as u32,
            responses: topic_responses,
        };
        let size = produce_response.get_size() + header.get_size();

        let body = FrameBody::ProduceResponse(produce_response);

        Ok(Frame { size, header, body })
    }
}
