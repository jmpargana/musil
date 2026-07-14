use bytes::{BufMut, Bytes, BytesMut};

use crate::protocol::{
    body::FrameBody,
    codec::{ParseError, RequestDecoder},
    header::RequestHeader,
};

pub mod body;
pub mod codec;
pub mod error_codes;
pub mod fetch;
pub mod header;
pub mod metadata;
pub mod produce;

pub struct Frame {
    pub size: u32,
    pub header: RequestHeader,
    pub body: FrameBody,
}

impl Frame {
    // This method might be redundant if using sendfile from producer...
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_u32(0); // size placeholder — backfilled below

        buf.put_u32(u32::from(self.header.api_key));
        buf.put_u32(self.header.api_version);
        buf.put_u32(self.header.correlation_id);

        match &self.header.client_id {
            Some(client_id) => {
                buf.put_i16(client_id.len() as i16);
                buf.put_slice(client_id.as_bytes());
            }
            None => buf.put_i16(-1),
        }

        match &self.body {
            FrameBody::Produce(req) => {
                buf.put_u64(req.transactional_id);
                buf.put_u32(u32::from(req.acks));

                buf.put_u64(req.timeout.as_millis() as u64);

                buf.put_u16(req.topics.len() as u16);
                for t in &req.topics {
                    buf.put_u16(t.topic.len() as u16);
                    buf.put_slice(t.topic.as_bytes());

                    buf.put_u32(t.partitions.len() as u32);
                    for p in &t.partitions {
                        buf.put_u16(p.index as u16);
                        let header = p.records.encode_header();
                        let batch_len = (header.len() + p.records.records.len()) as u32;
                        buf.put_u32(batch_len);
                        buf.put_slice(&header);
                        buf.put_slice(&p.records.records);
                    }
                }
            }
            FrameBody::Fetch(req) => {
                buf.put_i32(req.replica_id);
                buf.put_u32(req.max_bytes);
                buf.put_u32(req.topics.len() as u32);
                for t in &req.topics {
                    buf.put_u16(t.topic.len() as u16);
                    buf.put_slice(t.topic.as_bytes());
                    buf.put_u32(t.partitions.len() as u32);
                    for p in &t.partitions {
                        buf.put_u32(p.partition);
                        buf.put_u64(p.fetch_offset);
                        buf.put_u32(p.partition_max_bytes);
                        buf.put_u64(p.high_watermark);
                    }
                }
            }
            FrameBody::FetchResponse(resp) => {
                buf.put_u32(resp.throttle_time_ms);
                buf.put_u32(resp.responses.len() as u32);
                for t in &resp.responses {
                    buf.put_u16(t.topic.len() as u16);
                    buf.put_slice(t.topic.as_bytes());
                    buf.put_u32(t.partitions.len() as u32);
                    for p in &t.partitions {
                        buf.put_u32(p.partition_index);
                        buf.put_i16(i16::from(p.error_code));
                        buf.put_u64(p.high_watermark);
                        buf.put_u64(p.log_start_offset);
                        buf.put_u32(p.records.len() as u32);
                        for batch in &p.records {
                            let header = batch.encode_header();
                            buf.put_slice(&header);
                            buf.put_slice(&batch.records);
                        }
                    }
                }
            }
            FrameBody::ProduceResponse(resp) => {
                buf.put_u32(resp.throttle_time_ms);
                buf.put_u32(resp.responses.len() as u32);
                for t in &resp.responses {
                    buf.put_u16(t.topic.len() as u16);
                    buf.put_slice(t.topic.as_bytes());
                    buf.put_u32(t.partition_responses.len() as u32);
                    for p in &t.partition_responses {
                        buf.put_u32(p.index);
                        buf.put_i16(i16::from(p.error_code));
                        buf.put_u64(p.base_offset);
                        buf.put_u64(p.log_append_time_ms);
                        buf.put_u64(p.log_start_offset);
                        buf.put_i16(p.error_message.len() as i16);
                        buf.put_slice(p.error_message.as_bytes());
                        match &p.current_leader {
                            Some(leader) => {
                                buf.put_u8(1);
                                buf.put_i32(leader.leader_id);
                                buf.put_u32(leader.leader_epoch);
                            }
                            None => buf.put_u8(0),
                        }
                    }
                }
            }
            FrameBody::Metadata(req) => {
                buf.put_u32(req.topics.len() as u32);
                for t in &req.topics {
                    buf.put_i16(t.len() as i16);
                    buf.put_slice(t.as_bytes());
                }
                buf.put_u8(req.allow_auto_topic_creation as u8);
            }
            FrameBody::MetadataResponse(res) => {
                buf.put_u32(res.throttle_time_ms);
                buf.put_u32(res.brokers.len() as u32);
                for b in &res.brokers {
                    buf.put_i32(b.node_id);
                    buf.put_u16(b.host.len() as u16);
                    buf.put_slice(&b.host.as_bytes());
                    buf.put_i32(b.port);
                }
                buf.put_i32(res.controller_id);
                buf.put_u32(res.topics.len() as u32);
                for t in &res.topics {
                    buf.put_i16(i16::from(t.error_code));
                    buf.put_u16(t.name.len() as u16);
                    buf.put_slice(t.name.as_bytes());
                    buf.put_u32(t.partitions.len() as u32);
                    for p in &t.partitions {
                        buf.put_i16(i16::from(p.error_code));
                        buf.put_i32(p.partition_index);
                        buf.put_i32(p.leader_id);
                        buf.put_u32(p.replica_nodes);
                        buf.put_u32(p.isr_nodes);
                        buf.put_u32(p.offline_replicas);
                    }
                }
                buf.put_i16(i16::from(res.error_code));
            }
        }

        let size = (buf.len() - 4) as u32;
        buf[..4].copy_from_slice(&size.to_be_bytes());

        buf.freeze()
    }

    pub fn decode(buf: &Bytes, size: u32) -> Result<Self, ParseError> {
        let mut decoder = RequestDecoder;
        let mut buf = buf.clone();
        decoder.parse(&mut buf, size)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;

    use crate::{
        protocol::{
            Frame,
            body::FrameBody,
            fetch::request::{
                fetch_partition::FetchPartition, fetch_request::FetchRequest,
                fetch_topic::FetchTopic,
            },
            header::{ApiKey, RequestHeaderBuilder},
            metadata::MetadataRequest,
            produce::{
                acks::Acks,
                request::{
                    produce_partition::ProducePartition, produce_request::ProduceRequest,
                    produce_topic::ProduceTopic,
                },
            },
        },
        storage::{record::Record, record_batch::RecordBatch},
    };

    fn make_batch(base_offset: u64, records: &[(&[u8], &[u8])]) -> RecordBatch {
        let mut encoded = Vec::new();
        for (i, (k, v)) in records.iter().enumerate() {
            encoded.extend(Record::new(i as u64, k, v).encode());
        }
        RecordBatch {
            base_offset,
            batch_length: 4 + encoded.len() as u32,
            records_count: records.len() as u32,
            records: Bytes::from(encoded),
        }
    }

    fn make_header(
        api_key: ApiKey,
        correlation_id: u32,
        client_id: Option<&str>,
    ) -> crate::protocol::header::RequestHeader {
        RequestHeaderBuilder::default()
            .api_key(api_key)
            .api_version(0)
            .correlation_id(correlation_id)
            .client_id(client_id.map(|s| s.to_string()))
            .build()
            .unwrap()
    }

    fn produce_frame(
        correlation_id: u32,
        client_id: Option<&str>,
        acks: Acks,
        timeout: Duration,
        topics: Vec<ProduceTopic>,
    ) -> Frame {
        Frame {
            size: 0,
            header: make_header(ApiKey::Produce, correlation_id, client_id),
            body: FrameBody::Produce(ProduceRequest {
                transactional_id: 0,
                acks,
                timeout,
                topics,
            }),
        }
    }

    fn roundtrip(frame: Frame) -> Frame {
        let encoded = frame.encode();
        let size = u32::from_be_bytes(encoded[0..4].try_into().unwrap());
        let body = encoded.slice(4..);
        Frame::decode(&body, size).unwrap()
    }

    // --- header roundtrips ---

    #[test]
    fn encode_decode_correlation_id() {
        let batch = make_batch(0, &[]);
        let frame = produce_frame(
            77,
            None,
            Acks::None,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![ProducePartition {
                    index: 0,
                    records: batch,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        assert_eq!(decoded.header.correlation_id, 77);
    }

    #[test]
    fn encode_decode_client_id_some() {
        let batch = make_batch(0, &[]);
        let frame = produce_frame(
            1,
            Some("my-client"),
            Acks::None,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![ProducePartition {
                    index: 0,
                    records: batch,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        assert_eq!(decoded.header.client_id.as_deref(), Some("my-client"));
    }

    #[test]
    fn encode_decode_client_id_none() {
        let batch = make_batch(0, &[]);
        let frame = produce_frame(
            1,
            None,
            Acks::None,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![ProducePartition {
                    index: 0,
                    records: batch,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        assert_eq!(decoded.header.client_id, None);
    }

    #[test]
    fn encode_decode_api_key() {
        let batch = make_batch(0, &[]);
        let frame = produce_frame(
            1,
            None,
            Acks::None,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![ProducePartition {
                    index: 0,
                    records: batch,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        assert_eq!(decoded.header.api_key, ApiKey::Produce);
    }

    // --- produce body roundtrips ---

    #[test]
    fn encode_decode_acks_none() {
        let batch = make_batch(0, &[]);
        let frame = produce_frame(
            1,
            None,
            Acks::None,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![ProducePartition {
                    index: 0,
                    records: batch,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Produce(r) => assert_eq!(r.acks, Acks::None),
            _ => panic!(),
        }
    }

    #[test]
    fn encode_decode_acks_leader() {
        let batch = make_batch(0, &[]);
        let frame = produce_frame(
            1,
            None,
            Acks::Leader,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![ProducePartition {
                    index: 0,
                    records: batch,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Produce(r) => assert_eq!(r.acks, Acks::Leader),
            _ => panic!(),
        }
    }

    #[test]
    fn encode_decode_acks_all() {
        let batch = make_batch(0, &[]);
        let frame = produce_frame(
            1,
            None,
            Acks::All,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![ProducePartition {
                    index: 0,
                    records: batch,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Produce(r) => assert_eq!(r.acks, Acks::All),
            _ => panic!(),
        }
    }

    #[test]
    fn encode_decode_timeout_millis() {
        // Bug was as_secs() — 5500ms would round down to 5s then decode as 5000ms
        let batch = make_batch(0, &[]);
        let frame = produce_frame(
            1,
            None,
            Acks::None,
            Duration::from_millis(5500),
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![ProducePartition {
                    index: 0,
                    records: batch,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Produce(r) => assert_eq!(r.timeout.as_millis(), 5500),
            _ => panic!(),
        }
    }

    #[test]
    fn encode_decode_topic_name() {
        let batch = make_batch(0, &[]);
        let frame = produce_frame(
            1,
            None,
            Acks::None,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "orders".into(),
                partitions: vec![ProducePartition {
                    index: 0,
                    records: batch,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Produce(r) => assert_eq!(r.topics[0].topic, "orders"),
            _ => panic!(),
        }
    }

    #[test]
    fn encode_decode_partition_index() {
        let batch = make_batch(0, &[]);
        let frame = produce_frame(
            1,
            None,
            Acks::None,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![ProducePartition {
                    index: 7,
                    records: batch,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Produce(r) => assert_eq!(r.topics[0].partitions[0].index, 7),
            _ => panic!(),
        }
    }

    #[test]
    fn encode_decode_batch_fields() {
        let batch = make_batch(42, &[(b"key", b"val")]);
        let frame = produce_frame(
            1,
            None,
            Acks::None,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![ProducePartition {
                    index: 0,
                    records: batch,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Produce(r) => {
                let b = &r.topics[0].partitions[0].records;
                assert_eq!(b.base_offset, 42);
                assert_eq!(b.records_count, 1);
                let (rec, _) = Record::decode_raw(&b.records).unwrap();
                assert_eq!(rec.key, b"key");
                assert_eq!(rec.value, b"val");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn encode_decode_multiple_records_in_batch() {
        let batch = make_batch(0, &[(b"k0", b"v0"), (b"k1", b"v1"), (b"k2", b"v2")]);
        let frame = produce_frame(
            1,
            None,
            Acks::None,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![ProducePartition {
                    index: 0,
                    records: batch,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Produce(r) => {
                let b = &r.topics[0].partitions[0].records;
                assert_eq!(b.records_count, 3);
                let mut pos = 0;
                for expected_key in [b"k0".as_ref(), b"k1", b"k2"] {
                    let (rec, consumed) = Record::decode_raw(&b.records[pos..]).unwrap();
                    assert_eq!(rec.key, expected_key);
                    pos += consumed;
                }
            }
            _ => panic!(),
        }
    }

    #[test]
    fn encode_decode_multiple_partitions() {
        let b0 = make_batch(0, &[(b"p0-key", b"p0-val")]);
        let b1 = make_batch(0, &[(b"p1-key", b"p1-val")]);
        let frame = produce_frame(
            1,
            None,
            Acks::None,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![
                    ProducePartition {
                        index: 0,
                        records: b0,
                    },
                    ProducePartition {
                        index: 1,
                        records: b1,
                    },
                ],
            }],
        );
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Produce(r) => {
                let parts = &r.topics[0].partitions;
                assert_eq!(parts.len(), 2);
                let (r0, _) = Record::decode_raw(&parts[0].records.records).unwrap();
                assert_eq!(r0.key, b"p0-key");
                let (r1, _) = Record::decode_raw(&parts[1].records.records).unwrap();
                assert_eq!(r1.key, b"p1-key");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn encode_decode_multiple_topics() {
        let b0 = make_batch(0, &[(b"ka", b"va")]);
        let b1 = make_batch(0, &[(b"kb", b"vb")]);
        let frame = produce_frame(
            1,
            None,
            Acks::None,
            Duration::ZERO,
            vec![
                ProduceTopic {
                    topic: "topic-a".into(),
                    partitions: vec![ProducePartition {
                        index: 0,
                        records: b0,
                    }],
                },
                ProduceTopic {
                    topic: "topic-b".into(),
                    partitions: vec![ProducePartition {
                        index: 0,
                        records: b1,
                    }],
                },
            ],
        );
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Produce(r) => {
                assert_eq!(r.topics.len(), 2);
                assert_eq!(r.topics[0].topic, "topic-a");
                assert_eq!(r.topics[1].topic, "topic-b");
                let (r0, _) =
                    Record::decode_raw(&r.topics[0].partitions[0].records.records).unwrap();
                assert_eq!(r0.key, b"ka");
                let (r1, _) =
                    Record::decode_raw(&r.topics[1].partitions[0].records.records).unwrap();
                assert_eq!(r1.key, b"kb");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn encode_size_field_matches_payload() {
        let batch = make_batch(0, &[(b"k", b"v")]);
        let frame = produce_frame(
            1,
            Some("c"),
            Acks::None,
            Duration::ZERO,
            vec![ProduceTopic {
                topic: "t".into(),
                partitions: vec![ProducePartition {
                    index: 0,
                    records: batch,
                }],
            }],
        );
        let encoded = frame.encode();
        let declared_size = u32::from_be_bytes(encoded[0..4].try_into().unwrap());
        assert_eq!(declared_size as usize, encoded.len() - 4);
    }

    // --- fetch roundtrips ---

    fn fetch_frame(
        correlation_id: u32,
        replica_id: i32,
        max_bytes: u32,
        topics: Vec<FetchTopic>,
    ) -> Frame {
        Frame {
            size: 0,
            header: make_header(ApiKey::Fetch, correlation_id, None),
            body: FrameBody::Fetch(FetchRequest {
                replica_id,
                max_bytes,
                topics,
            }),
        }
    }

    #[test]
    fn fetch_encode_decode_header() {
        let frame = fetch_frame(55, -1, 1024, vec![]);
        let decoded = roundtrip(frame);
        assert_eq!(decoded.header.correlation_id, 55);
        assert_eq!(decoded.header.api_key, ApiKey::Fetch);
    }

    #[test]
    fn fetch_encode_decode_fields() {
        let frame = fetch_frame(
            1,
            -1,
            65536,
            vec![FetchTopic {
                topic: "events".into(),
                partitions: vec![FetchPartition {
                    partition: 3,
                    fetch_offset: 100,
                    partition_max_bytes: 4096,
                    high_watermark: 0,
                }],
            }],
        );
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Fetch(r) => {
                assert_eq!(r.replica_id, -1);
                assert_eq!(r.max_bytes, 65536);
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].topic, "events");
                assert_eq!(r.topics[0].partitions[0].partition, 3);
                assert_eq!(r.topics[0].partitions[0].fetch_offset, 100);
                assert_eq!(r.topics[0].partitions[0].partition_max_bytes, 4096);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn fetch_encode_decode_multiple_topics_and_partitions() {
        let frame = fetch_frame(
            1,
            0,
            1024,
            vec![
                FetchTopic {
                    topic: "topic-a".into(),
                    partitions: vec![
                        FetchPartition {
                            partition: 0,
                            fetch_offset: 0,
                            partition_max_bytes: 512,
                            high_watermark: 0,
                        },
                        FetchPartition {
                            partition: 1,
                            fetch_offset: 10,
                            partition_max_bytes: 512,
                            high_watermark: 0,
                        },
                    ],
                },
                FetchTopic {
                    topic: "topic-b".into(),
                    partitions: vec![FetchPartition {
                        partition: 0,
                        fetch_offset: 5,
                        partition_max_bytes: 256,
                        high_watermark: 0,
                    }],
                },
            ],
        );
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Fetch(r) => {
                assert_eq!(r.topics.len(), 2);
                assert_eq!(r.topics[0].topic, "topic-a");
                assert_eq!(r.topics[0].partitions.len(), 2);
                assert_eq!(r.topics[0].partitions[1].fetch_offset, 10);
                assert_eq!(r.topics[1].topic, "topic-b");
                assert_eq!(r.topics[1].partitions[0].fetch_offset, 5);
            }
            _ => panic!(),
        }
    }

    // --- metadata roundtrips ---

    fn metadata_frame(topics: Vec<String>, allow_auto_topic_creation: bool) -> Frame {
        Frame {
            size: 0,
            header: make_header(ApiKey::Metadata, 1, None),
            body: FrameBody::Metadata(MetadataRequest {
                topics,
                allow_auto_topic_creation,
            }),
        }
    }

    #[test]
    fn metadata_encode_decode_topics() {
        let frame = metadata_frame(vec!["orders".into(), "events".into()], false);
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Metadata(r) => {
                assert_eq!(r.topics, vec!["orders", "events"]);
                assert!(!r.allow_auto_topic_creation);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn metadata_encode_decode_allow_auto_creation() {
        let frame = metadata_frame(vec![], true);
        let decoded = roundtrip(frame);
        match decoded.body {
            FrameBody::Metadata(r) => {
                assert!(r.allow_auto_topic_creation);
                assert!(r.topics.is_empty());
            }
            _ => panic!(),
        }
    }

    #[test]
    fn metadata_encode_size_field_matches_payload() {
        let frame = metadata_frame(vec!["t".into()], false);
        let encoded = frame.encode();
        let declared_size = u32::from_be_bytes(encoded[0..4].try_into().unwrap());
        assert_eq!(declared_size as usize, encoded.len() - 4);
    }
}
