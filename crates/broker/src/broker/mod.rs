use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
    time::Instant,
};

use arc_swap::ArcSwap;
use tokio::{
    sync::mpsc::{self, channel},
    task::JoinHandle,
};

use storage::{
    partition::handle::PartitionHandle,
    topic::TopicPartition,
};
use network::protocol::{
    Frame,
    body::FrameBody,
    error_codes::ErrorCode,
    fetch::response::{
        fetch_response::FetchResponse, partition_response::PartitionResponse,
        topic_response::TopicResponse,
    },
    metadata::{MetadataResponse, PartitionMetadata, TopicMetadata},
    produce::response::{
        partition_response::ProducePartitionResponse, produce_response::ProduceResponse,
        topic_response::ProduceTopicResponse,
    },
};

use crate::broker::{
    actor::MetadataActor, command::MetadataCommand, config::BrokerConfig, state::MetadataImage,
};

pub mod actor;
pub mod command;
pub mod config;
pub mod metadata_record;
pub mod state;

pub struct Broker {
    pub state: Arc<ArcSwap<MetadataImage>>,
    pub config: BrokerConfig,
    brokers: Vec<BrokerConfig>,
    tx: mpsc::Sender<MetadataCommand>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl Broker {
    pub fn new(path: String, config: BrokerConfig, brokers: Vec<BrokerConfig>) -> Self {
        let (tx, rx) = channel(100);

        let mut actor = MetadataActor::new(rx, path);
        let state = actor.snapshot();
        let join = tokio::spawn(async move {
            actor.run().await;
        });

        Self {
            state,
            config,
            brokers,
            tx,
            join: Mutex::new(Some(join)),
        }
    }

    pub fn with_partitions(partitions: HashMap<TopicPartition, Arc<PartitionHandle>>) -> Self {
        use crate::broker::config::BrokerConfigBuilder;

        let (tx, _rx) = channel(100);
        let mut image = MetadataImage::empty();
        image.partitions = partitions;

        Self {
            state: Arc::new(ArcSwap::from_pointee(image)),
            config: BrokerConfigBuilder::default()
                .node_id(1)
                .host("localhost".to_string())
                .port(9092)
                .topics(vec![])
                .build()
                .unwrap(),
            brokers: vec![],
            tx,
            join: Mutex::new(None),
        }
    }

    pub fn partition(&self, topic: &str, partition: u16) -> Option<Arc<PartitionHandle>> {
        self.state
            .load_full()
            .partitions
            .get(&TopicPartition { topic_id: topic.to_owned(), partition_id: partition })
            .cloned()
    }

    pub async fn handle(&self, req: Frame) -> io::Result<Frame> {
        match &req.body {
            FrameBody::Fetch(_) => self.handle_fetch(req).await,
            FrameBody::Produce(_) => self.handle_produce(req).await,
            FrameBody::Metadata(_) => self.handle_metadata(req).await,
            FrameBody::Topic(_) => self.handle_create_topics(req).await,
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "unsupported frame type")),
        }
    }

    async fn handle_fetch(&self, req: Frame) -> io::Result<Frame> {
        let FrameBody::Fetch(body) = req.body else { unreachable!() };
        let now = Instant::now();
        let mut topic_responses = Vec::new();

        for t in body.topics {
            let mut part_responses = Vec::new();
            for p in t.partitions {
                let part_res = match self.partition(&t.topic, p.partition as u16) {
                    Some(partition) => partition.fetch(p, body.replica_id).await,
                    None => PartitionResponse::error(p.partition, ErrorCode::UnknownTopicOrPartition),
                };
                part_responses.push(part_res);
            }
            topic_responses.push(TopicResponse { topic: t.topic.clone(), partitions: part_responses });
        }

        let fetch_response = FetchResponse {
            throttle_time_ms: now.elapsed().as_millis() as u32,
            responses: topic_responses,
        };

        let header = req.header.clone();
        let size = fetch_response.get_size() + header.get_size();

        Ok(Frame { size, header, body: FrameBody::FetchResponse(fetch_response) })
    }

    async fn handle_produce(&self, req: Frame) -> io::Result<Frame> {
        let FrameBody::Produce(body) = req.body else { unreachable!() };
        let now = Instant::now();
        let mut topic_responses = Vec::new();

        for t in body.topics {
            let mut part_responses = Vec::new();
            for p in t.partitions {
                let part_res = match self.partition(&t.topic, p.index as u16) {
                    Some(partition) => partition.append(p.records, body.acks).await,
                    None => ProducePartitionResponse::error(p.index as u32, ErrorCode::UnknownTopicOrPartition),
                };
                part_responses.push(part_res);
            }
            topic_responses.push(ProduceTopicResponse { topic: t.topic, partition_responses: part_responses });
        }

        let header = req.header.clone();
        let produce_response = ProduceResponse {
            throttle_time_ms: now.elapsed().as_millis() as u32,
            responses: topic_responses,
        };
        let size = produce_response.get_size() + header.get_size();

        Ok(Frame { size, header, body: FrameBody::ProduceResponse(produce_response) })
    }

    async fn handle_metadata(&self, req: Frame) -> io::Result<Frame> {
        let FrameBody::Metadata(_body) = req.body else { unreachable!() };
        let now = Instant::now();

        let mut topics: HashMap<String, Vec<PartitionMetadata>> = HashMap::new();

        let metadata = self.state.load_full();
        for (topic_partition, handle) in &metadata.partitions {
            let state = handle.state.load_full();

            let mut isr = 0;
            let mut offline = 0;

            state.replicas.iter().for_each(|replica| {
                if replica.is_in_sync { isr += 1; } else { offline += 1; }
            });

            let pm = PartitionMetadata {
                error_code: ErrorCode::None,
                partition_index: topic_partition.partition_id as i32,
                leader_id: 0,
                replica_nodes: state.replicas.len() as u32,
                isr_nodes: isr,
                offline_replicas: offline,
            };

            topics.entry(topic_partition.topic_id.clone()).or_default().push(pm);
        }

        let topics = topics
            .into_iter()
            .map(|(k, v)| TopicMetadata { partitions: v, error_code: ErrorCode::None, name: k })
            .collect();

        let res = MetadataResponse {
            throttle_time_ms: now.elapsed().as_millis() as u32,
            brokers: self.brokers.iter().map(|b| b.into()).collect(),
            controller_id: 0,
            topics,
            error_code: ErrorCode::None,
        };

        let header = req.header.clone();
        let size = header.get_size() + res.get_size();

        Ok(Frame { size, header, body: FrameBody::MetadataResponse(res) })
    }

    async fn handle_create_topics(&self, req: Frame) -> io::Result<Frame> {
        let FrameBody::Topic(body) = req.body else { unreachable!() };

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(MetadataCommand::CreateTopic { req: body, done: done_tx })
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e.to_string()))?;

        let res = done_rx
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e.to_string()))?;

        let header = req.header.clone();
        let size = header.get_size() + res.get_size();

        Ok(Frame { size, header, body: FrameBody::TopicResponse(res) })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;

    use storage::partition::config::PartitionConfigBuilder;
    use storage::partition::handle::PartitionHandle;
    use storage::topic::TopicPartition;
    use network::protocol::Frame;
    use network::protocol::body::FrameBody;
    use network::protocol::error_codes::ErrorCode;
    use network::protocol::fetch::request::fetch_partition::FetchPartition;
    use network::protocol::fetch::request::fetch_request::FetchRequest;
    use network::protocol::fetch::request::fetch_topic::FetchTopic;
    use network::protocol::header::{ApiKey, RequestHeaderBuilder};
    use network::protocol::produce::acks::Acks;
    use network::protocol::produce::request::produce_partition::ProducePartition;
    use network::protocol::produce::request::produce_request::ProduceRequest;
    use network::protocol::produce::request::produce_topic::ProduceTopic;
    use network::protocol::produce::response::produce_response::ProduceResponse;
    use proto::record::Record;
    use proto::record_batch::RecordBatch;

    use super::Broker;

    fn make_partition(
        dir: &tempdir::TempDir,
        topic: &str,
        partition_id: u16,
    ) -> std::sync::Arc<PartitionHandle> {
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

    fn make_header(api_key: ApiKey) -> network::protocol::header::RequestHeader {
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
        let records = Bytes::from(encoded);
        let batch_length = 4 + records.len() as u32;
        RecordBatch::from_compact(base_offset, batch_length, 1, records)
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
                        partition_max_bytes: 1 << 20,
                        high_watermark: 0,
                    }],
                }],
            }),
        }
    }

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
        broker
            .handle(produce_frame("orders", 0, record_batch(0, b"key", b"val")))
            .await
            .unwrap();
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

    #[tokio::test]
    async fn handle_response_frame_returns_error_not_panic() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[]);
        let frame = Frame {
            size: 0,
            header: make_header(ApiKey::Produce),
            body: FrameBody::ProduceResponse(ProduceResponse { throttle_time_ms: 0, responses: vec![] }),
        };
        assert!(broker.handle(frame).await.is_err());
    }

    fn metadata_frame() -> Frame {
        Frame {
            size: 0,
            header: make_header(ApiKey::Metadata),
            body: FrameBody::Metadata(network::protocol::metadata::MetadataRequest {
                topics: vec![],
                allow_auto_topic_creation: false,
            }),
        }
    }

    #[tokio::test]
    async fn metadata_returns_metadata_response() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0])]);
        let resp = broker.handle(metadata_frame()).await.unwrap();
        assert!(matches!(resp.body, FrameBody::MetadataResponse(_)));
    }

    #[tokio::test]
    async fn metadata_response_lists_all_partitions() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0, 1]), ("events", &[0])]);
        let resp = broker.handle(metadata_frame()).await.unwrap();
        match resp.body {
            FrameBody::MetadataResponse(r) => {
                let total_partitions: usize = r.topics.iter().map(|t| t.partitions.len()).sum();
                assert_eq!(total_partitions, 3);
            }
            _ => panic!("expected MetadataResponse"),
        }
    }

    #[tokio::test]
    async fn metadata_response_preserves_correlation_id() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0])]);
        let mut frame = metadata_frame();
        frame.header.correlation_id = 99;
        let resp = broker.handle(frame).await.unwrap();
        assert_eq!(resp.header.correlation_id, 99);
    }

    #[tokio::test]
    async fn metadata_response_no_error_code() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0])]);
        let resp = broker.handle(metadata_frame()).await.unwrap();
        match resp.body {
            FrameBody::MetadataResponse(r) => {
                assert_eq!(r.error_code, ErrorCode::None);
                for topic in &r.topics {
                    assert_eq!(topic.error_code, ErrorCode::None);
                    for partition in &topic.partitions {
                        assert_eq!(partition.error_code, ErrorCode::None);
                    }
                }
            }
            _ => panic!("expected MetadataResponse"),
        }
    }

    #[tokio::test]
    async fn metadata_response_empty_broker_has_no_topics() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[]);
        let resp = broker.handle(metadata_frame()).await.unwrap();
        match resp.body {
            FrameBody::MetadataResponse(r) => assert!(r.topics.is_empty()),
            _ => panic!("expected MetadataResponse"),
        }
    }

    #[tokio::test]
    async fn metadata_response_size_matches_encoded_payload() {
        let dir = tempdir::TempDir::new("broker-test").unwrap();
        let broker = make_broker(&dir, &[("orders", &[0, 1])]);
        let resp = broker.handle(metadata_frame()).await.unwrap();
        let encoded = resp.encode();
        let declared_size = u32::from_be_bytes(encoded[0..4].try_into().unwrap());
        assert_eq!(declared_size as usize, encoded.len() - 4);
    }

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
