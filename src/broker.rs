use std::{collections::HashMap, io, sync::Arc, time::Instant};

use crate::{
    partition::handle::PartitionHandle,
    protocol::{
        Frame,
        body::FrameBody::{self},
        error_codes::ErrorCode,
        fetch::{
            response::{
                fetch_response::FetchResponse,
                partition_response::PartitionResponse,
                topic_response::TopicResponse,
            },
        },
        produce::{
            response::{
                partition_response::ProducePartitionResponse,
                produce_response::ProduceResponse,
                topic_response::ProduceTopicResponse,
            },
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

    pub fn with_partitions(partitions: HashMap<TopicPartition, Arc<PartitionHandle>>) -> Self {
        Self { partitions }
    }

    pub fn update(&mut self) {}

    pub fn partition(&self, topic: &str, partition: u16) -> Option<&Arc<PartitionHandle>> {
        self.partitions.get(&TopicPartition {
            topic_id: topic.to_owned(),
            partition_id: partition,
        })
    }

    pub async fn handle(&self, req: Frame) -> io::Result<Frame> {
        match &req.body {
            FrameBody::Fetch(_) => self.handle_fetch(req).await,
            FrameBody::Produce(_) => self.handle_produce(req).await,
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "unsupported frame type")),
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
                let part_res = match self.partition(&t.topic, p.partition as u16) {
                    Some(partition) => partition.fetch(p, body.replica_id).await,
                    None => PartitionResponse::error(
                        p.partition,
                        ErrorCode::UnknownTopicOrPartition,
                    ),
                };
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
                let part_res = match self.partition(&t.topic, p.index as u16) {
                    Some(partition) => partition.append(p.records, body.acks).await,
                    None => ProducePartitionResponse::error(
                        p.index as u32,
                        ErrorCode::UnknownTopicOrPartition,
                    ),
                };
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;

    use crate::partition::config::PartitionConfigBuilder;
    use crate::partition::handle::PartitionHandle;
    use crate::protocol::body::FrameBody;
    use crate::protocol::error_codes::ErrorCode;
    use crate::protocol::fetch::request::fetch_partition::FetchPartition;
    use crate::protocol::fetch::request::fetch_request::FetchRequest;
    use crate::protocol::fetch::request::fetch_topic::FetchTopic;
    use crate::protocol::header::{ApiKey, RequestHeaderBuilder};
    use crate::protocol::produce::acks::Acks;
    use crate::protocol::produce::request::produce_partition::ProducePartition;
    use crate::protocol::produce::request::produce_request::ProduceRequest;
    use crate::protocol::produce::request::produce_topic::ProduceTopic;
    use crate::protocol::produce::response::produce_response::ProduceResponse;
    use crate::storage::record::Record;
    use crate::storage::record_batch::RecordBatch;
    use crate::topic::TopicPartition;
    use crate::protocol::Frame;

    use super::Broker;

    fn make_partition(dir: &tempdir::TempDir, topic: &str, partition_id: u16) -> std::sync::Arc<PartitionHandle> {
        let cfg = PartitionConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .topic_id(topic.to_string())
            .partition_id(partition_id)
            .broker_id(1)
            .segment_bytes(1 << 20)
            .build()
            .unwrap();
        PartitionHandle::spawn(partition_id as u32, cfg)
    }

    fn make_broker(dir: &tempdir::TempDir, topics: &[(&str, &[u16])]) -> Broker {
        let mut partitions = HashMap::new();
        for (topic, ids) in topics {
            for &id in *ids {
                partitions.insert(
                    TopicPartition { topic_id: topic.to_string(), partition_id: id },
                    make_partition(dir, topic, id),
                );
            }
        }
        Broker::with_partitions(partitions)
    }

    fn make_header(api_key: ApiKey) -> crate::protocol::header::RequestHeader {
        RequestHeaderBuilder::default()
            .api_key(api_key)
            .api_version(0)
            .correlation_id(1)
            .client_id(None)
            .build()
            .unwrap()
    }

    fn record_batch(base_offset: u64, key: &[u8], val: &[u8]) -> RecordBatch {
        let encoded = Record::new(0, key, val).encode();
        RecordBatch {
            base_offset,
            batch_length: 4 + encoded.len() as u32,
            records_count: 1,
            records: Bytes::from(encoded),
        }
    }

    fn produce_frame(topic: &str, partition_id: u16, batch: RecordBatch) -> Frame {
        Frame {
            size: 0,
            header: make_header(ApiKey::Produce),
            body: FrameBody::Produce(ProduceRequest {
                transactional_id: 0,
                acks: Acks::Leader,
                timeout: std::time::Duration::ZERO,
                topics: vec![ProduceTopic {
                    topic: topic.to_string(),
                    partitions: vec![ProducePartition { index: partition_id, records: batch }],
                }],
            }),
        }
    }

    fn fetch_frame(topic: &str, partition_id: u16, offset: u64) -> Frame {
        Frame {
            size: 0,
            header: make_header(ApiKey::Fetch),
            body: FrameBody::Fetch(FetchRequest {
                replica_id: -1,
                max_bytes: 1 << 20,
                topics: vec![FetchTopic {
                    topic: topic.to_string(),
                    partitions: vec![FetchPartition {
                        partition: partition_id as u32,
                        fetch_offset: offset,
                        log_start_offset: None,
                        partition_max_bytes: 1 << 20,
                        high_watermark: None,
                    }],
                }],
            }),
        }
    }

    // --- partition lookup ---

    #[tokio::test]
    async fn partition_found() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0])]);
        assert!(broker.partition("orders", 0).is_some());
    }

    #[tokio::test]
    async fn partition_not_found_returns_none() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0])]);
        assert!(broker.partition("orders", 1).is_none());
        assert!(broker.partition("unknown", 0).is_none());
    }

    // --- handle_produce ---

    #[tokio::test]
    async fn produce_returns_produce_response() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0])]);

        let frame = produce_frame("orders", 0, record_batch(0, b"k", b"v"));
        let resp = broker.handle(frame).await.unwrap();

        assert!(matches!(resp.body, FrameBody::ProduceResponse(_)));
    }

    #[tokio::test]
    async fn produce_response_contains_topic_and_partition() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0])]);

        let frame = produce_frame("orders", 0, record_batch(0, b"k", b"v"));
        let resp = broker.handle(frame).await.unwrap();

        match resp.body {
            FrameBody::ProduceResponse(r) => {
                assert_eq!(r.responses.len(), 1);
                assert_eq!(r.responses[0].topic, "orders");
                assert_eq!(r.responses[0].partition_responses.len(), 1);
                assert_eq!(r.responses[0].partition_responses[0].base_offset, 0);
            }
            _ => panic!("expected ProduceResponse"),
        }
    }

    #[tokio::test]
    async fn produce_unknown_partition_returns_error_code() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0])]);

        let frame = produce_frame("orders", 99, record_batch(0, b"k", b"v"));
        let resp = broker.handle(frame).await.unwrap();
        match resp.body {
            FrameBody::ProduceResponse(r) => {
                assert_eq!(
                    r.responses[0].partition_responses[0].error_code,
                    ErrorCode::UnknownTopicOrPartition
                );
            }
            _ => panic!("expected ProduceResponse"),
        }
    }

    #[tokio::test]
    async fn produce_preserves_correlation_id() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0])]);

        let mut frame = produce_frame("orders", 0, record_batch(0, b"k", b"v"));
        frame.header.correlation_id = 42;

        let resp = broker.handle(frame).await.unwrap();
        assert_eq!(resp.header.correlation_id, 42);
    }

    // --- handle_fetch ---

    #[tokio::test]
    async fn fetch_returns_fetch_response() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0])]);

        let frame = fetch_frame("orders", 0, 0);
        let resp = broker.handle(frame).await.unwrap();

        assert!(matches!(resp.body, FrameBody::FetchResponse(_)));
    }

    #[tokio::test]
    async fn fetch_after_produce_returns_written_record() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0])]);

        broker.handle(produce_frame("orders", 0, record_batch(0, b"key", b"val"))).await.unwrap();

        let resp = broker.handle(fetch_frame("orders", 0, 0)).await.unwrap();
        match resp.body {
            FrameBody::FetchResponse(r) => {
                assert_eq!(r.responses.len(), 1);
                let batches = &r.responses[0].partitions[0].records;
                assert!(!batches.is_empty());
                let (rec, _) = Record::decode_raw(&batches[0].records).unwrap();
                assert_eq!(rec.key, b"key");
                assert_eq!(rec.value, b"val");
            }
            _ => panic!("expected FetchResponse"),
        }
    }

    #[tokio::test]
    async fn fetch_unknown_partition_returns_error_code() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0])]);

        let frame = fetch_frame("orders", 99, 0);
        let resp = broker.handle(frame).await.unwrap();
        match resp.body {
            FrameBody::FetchResponse(r) => {
                assert_eq!(
                    r.responses[0].partitions[0].error_code,
                    ErrorCode::UnknownTopicOrPartition
                );
            }
            _ => panic!("expected FetchResponse"),
        }
    }

    // --- handle rejects response frames ---
    // Bug fixed: was panic!("unsupported"), now returns Err.

    #[tokio::test]
    async fn handle_response_frame_returns_error_not_panic() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[]);

        let frame = Frame {
            size: 0,
            header: make_header(ApiKey::Produce),
            body: FrameBody::ProduceResponse(ProduceResponse {
                throttle_time_ms: 0,
                responses: vec![],
            }),
        };
        assert!(broker.handle(frame).await.is_err());
    }

    // --- multiple partitions in one request ---

    #[tokio::test]
    async fn produce_multiple_partitions_all_get_responses() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0, 1])]);

        let frame = Frame {
            size: 0,
            header: make_header(ApiKey::Produce),
            body: FrameBody::Produce(ProduceRequest {
                transactional_id: 0,
                acks: Acks::Leader,
                timeout: std::time::Duration::ZERO,
                topics: vec![ProduceTopic {
                    topic: "orders".to_string(),
                    partitions: vec![
                        ProducePartition { index: 0, records: record_batch(0, b"k0", b"v0") },
                        ProducePartition { index: 1, records: record_batch(0, b"k1", b"v1") },
                    ],
                }],
            }),
        };

        let resp = broker.handle(frame).await.unwrap();
        match resp.body {
            FrameBody::ProduceResponse(r) => {
                assert_eq!(r.responses[0].partition_responses.len(), 2);
            }
            _ => panic!(),
        }
    }
}
