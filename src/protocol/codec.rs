use std::time::Duration;

use bytes::{Buf, Bytes};

use crate::{
    protocol::{
        Frame,
        fetch::request::{
            fetch_partition::FetchPartition, fetch_request::FetchRequest, fetch_topic::FetchTopic,
        },
        header::{ApiKey, RequestHeader},
        metadata::MetadataRequest,
        produce::request::{
            produce_partition::ProducePartition, produce_request::ProduceRequest,
            produce_topic::ProduceTopic,
        },
    },
    storage::record_batch::RecordBatch,
};

use super::body::FrameBody;

// RequestDecoder doesn't own buffer, instead it consumes just enough to find the size and then creates an event with fd ptr and size
#[derive(Debug)]
pub struct RequestDecoder;

#[derive(Debug)]
pub enum ParseError {
    InvalidApiKey,
    InvalidAck,
    InvalidClientId,
}

impl RequestDecoder {
    // `size`` is now decoded in the handler, so a frame can be allocated with the correct size.
    pub fn parse(&mut self, buf: &mut Bytes, size: u32) -> Result<Frame, ParseError> {
        let api_key = buf.get_u32();
        let api_version = buf.get_u32();
        let correlation_id = buf.get_u32();

        let client_id_len = buf.get_i16();
        let client_id = if client_id_len >= 0 {
            Some(
                String::from_utf8(buf.split_to(client_id_len as usize).to_vec())
                    .map_err(|_| ParseError::InvalidClientId)?,
            )
        } else {
            None
        };

        let api_key: ApiKey = api_key.try_into().map_err(|_| ParseError::InvalidApiKey)?;

        let header = RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        };

        // TODO: depending on message type we need to read different values from body
        let body: FrameBody = match api_key {
            ApiKey::Produce => self.parse_produce(buf)?,
            ApiKey::Fetch => self.parse_fetch(buf)?,
            ApiKey::Metadata => self.parse_metadata(buf)?,
        };

        Ok(Frame { size, header, body })
    }

    fn parse_produce(&self, buf: &mut Bytes) -> Result<FrameBody, ParseError> {
        let transactional_id = buf.get_u64();
        let acks = buf
            .get_u32()
            .try_into()
            .map_err(|_| ParseError::InvalidAck)?;

        let timeout = Duration::from_millis(buf.get_u64());

        let mut topics = Vec::new();

        let topic_size = buf.get_u16();
        for _ in 0..topic_size as usize {
            let topic_name_length = buf.get_u16();
            // TODO: can also use slice(..topic_name_length);
            let bytes = buf.split_to(topic_name_length as usize);
            let topic = String::from_utf8_lossy(&bytes.to_vec()).to_string();

            let partition_length = buf.get_u32();

            let mut partitions = Vec::new();
            for _ in 0..partition_length {
                let partition_id = buf.get_u16();

                let batch_len = buf.get_u32() as usize;
                let batch_bytes = buf.split_to(batch_len);
                let record_batch = RecordBatch::decode(&batch_bytes, 0);

                partitions.push(ProducePartition {
                    index: partition_id,
                    records: record_batch,
                });
            }

            topics.push(ProduceTopic { topic, partitions });
        }

        Ok(FrameBody::Produce(ProduceRequest {
            transactional_id,
            acks,
            timeout,
            topics,
        }))
    }

    fn parse_fetch(&self, buf: &mut Bytes) -> Result<FrameBody, ParseError> {
        let replica_id = buf.get_i32();
        let max_bytes = buf.get_u32();
        let topics_len = buf.get_u32();

        let mut topics = Vec::new();
        for _ in 0..topics_len {
            let topic_name_len = buf.get_u16();
            let topic = buf.split_to(topic_name_len as usize);
            let topic = String::from_utf8_lossy(&topic).to_string();
            let partitions_len = buf.get_u32();

            let mut partitions = Vec::new();

            for _ in 0..partitions_len {
                let partition = buf.get_u32();
                let fetch_offset = buf.get_u64();
                let partition_max_bytes = buf.get_u32();
                let high_watermark = buf.get_u64();

                partitions.push(FetchPartition {
                    partition,
                    fetch_offset,
                    partition_max_bytes,
                    high_watermark,
                })
            }

            topics.push(FetchTopic { topic, partitions });
        }
        Ok(FrameBody::Fetch(FetchRequest {
            replica_id,
            max_bytes,
            topics,
        }))
    }

    fn parse_metadata(&self, buf: &mut Bytes) -> Result<FrameBody, ParseError> {
        let topic_len = buf.get_u32();

        let mut topics = Vec::new();
        for _ in 0..topic_len {
            let topic_name_len = buf.get_i16();
            let topic = buf.split_to(topic_name_len as usize);
            let topic = String::from_utf8_lossy(&topic);
            topics.push(topic.to_string());
        }

        let allow_auto_topic_creation = if buf.get_u8() == 0 { false } else { true };

        Ok(FrameBody::Metadata(MetadataRequest {
            allow_auto_topic_creation,
            topics,
        }))
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use crate::protocol::{
        body::FrameBody, codec::RequestDecoder, header::ApiKey, produce::acks::Acks,
    };
    use crate::storage::record::Record;
    use crate::storage::record_batch::RecordBatch;

    use super::*;

    // Parse a full wire frame (4-byte size prefix + body) the same way connection.rs does.
    fn parse_full_frame(frame_bytes: Bytes) -> Result<Frame, ParseError> {
        let size = u32::from_be_bytes(frame_bytes[0..4].try_into().unwrap());
        let mut body = frame_bytes.slice(4..);
        RequestDecoder.parse(&mut body, size)
    }

    // Encode a RecordBatch to the wire format the codec expects:
    // u32 length prefix + 16-byte header + records payload.
    fn encode_batch_for_wire(batch: &RecordBatch) -> Vec<u8> {
        let header = batch.encode_header();
        let total = header.len() + batch.records.len();
        let mut out = Vec::new();
        out.extend_from_slice(&(total as u32).to_be_bytes());
        out.extend_from_slice(&header);
        out.extend_from_slice(&batch.records);
        out
    }

    fn make_batch(base_offset: u64, records: &[(&[u8], &[u8])]) -> RecordBatch {
        let mut encoded = Vec::new();
        for (i, (key, val)) in records.iter().enumerate() {
            encoded.extend(Record::new(i as u64, key, val).encode());
        }
        RecordBatch {
            base_offset,
            batch_length: 4 + encoded.len() as u32,
            records_count: records.len() as u32,
            records: Bytes::from(encoded),
        }
    }

    fn build_frame(
        correlation_id: u32,
        client_id: Option<&[u8]>,
        transactional_id: u64,
        acks: u32,
        timeout_ms: u64,
        topics: &[(&[u8], &[(u16, &[u8])])], // (topic_name, [(partition_id, wire_batch_bytes)])
    ) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_u32(0); // size placeholder
        buf.put_u32(0); // ApiKey::Produce = 0
        buf.put_u32(0); // version
        buf.put_u32(correlation_id);

        match client_id {
            Some(id) => {
                buf.put_i16(id.len() as i16);
                buf.extend_from_slice(id);
            }
            None => buf.put_i16(-1),
        }

        buf.put_u64(transactional_id);
        buf.put_u32(acks);
        buf.put_u64(timeout_ms);

        buf.put_u16(topics.len() as u16);
        for (topic_name, partitions) in topics {
            buf.put_u16(topic_name.len() as u16);
            buf.extend_from_slice(topic_name);
            buf.put_u32(partitions.len() as u32);
            for (partition_id, batch_wire) in *partitions {
                buf.put_u16(*partition_id);
                buf.extend_from_slice(batch_wire);
            }
        }

        let size = (buf.len() - 4) as u32;
        buf[..4].copy_from_slice(&size.to_be_bytes());

        buf.freeze()
    }

    // --- header parsing ---

    #[test]
    fn parses_header_fields() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(42, Some(b"client"), 0, 0, 0, &[(b"t", &[(0, &wire)])]);

        let frame = parse_full_frame(frame_bytes).unwrap();

        assert_eq!(frame.header.api_key, ApiKey::Produce);
        assert_eq!(frame.header.correlation_id, 42);
        assert_eq!(frame.header.client_id.as_deref(), Some("client"));
    }

    #[test]
    fn parses_null_client_id() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(1, None, 0, 0, 0, &[(b"t", &[(0, &wire)])]);

        let frame = parse_full_frame(frame_bytes).unwrap();
        assert_eq!(frame.header.client_id, None);
    }

    #[test]
    fn rejects_invalid_api_key() {
        let mut buf = BytesMut::new();
        buf.put_u32(8);
        buf.put_u32(0xFF); // unknown api key
        buf.put_u32(0);
        buf.put_u32(0);
        buf.put_i16(-1);

        let result = parse_full_frame(buf.freeze());
        assert!(matches!(result, Err(ParseError::InvalidApiKey)));
    }

    #[test]
    fn rejects_invalid_ack_value() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(1, None, 0, 99, 0, &[(b"t", &[(0, &wire)])]); // acks=99 invalid

        let result = parse_full_frame(frame_bytes);
        assert!(matches!(result, Err(ParseError::InvalidAck)));
    }

    // --- produce body parsing ---

    #[test]
    fn parses_produce_body_fields() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(1, None, 0xDEAD, 1, 5000, &[(b"orders", &[(3, &wire)])]);

        let frame = parse_full_frame(frame_bytes).unwrap();
        match frame.body {
            FrameBody::Produce(req) => {
                assert_eq!(req.transactional_id, 0xDEAD);
                assert_eq!(req.acks, Acks::Leader);
                assert_eq!(req.timeout.as_millis(), 5000);
            }
            _ => panic!("expected Produce"),
        }
    }

    #[test]
    fn parses_acks_none() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(1, None, 0, 0, 0, &[(b"t", &[(0, &wire)])]);
        let frame = parse_full_frame(frame_bytes).unwrap();
        match frame.body {
            FrameBody::Produce(req) => assert_eq!(req.acks, Acks::None),
            _ => panic!(),
        }
    }

    #[test]
    fn parses_acks_all() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(1, None, 0, 2, 0, &[(b"t", &[(0, &wire)])]);
        let frame = parse_full_frame(frame_bytes).unwrap();
        match frame.body {
            FrameBody::Produce(req) => assert_eq!(req.acks, Acks::All),
            _ => panic!(),
        }
    }

    // --- topic + partition structure ---

    #[test]
    fn parses_topic_name_and_partition_index() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(1, None, 0, 0, 0, &[(b"orders", &[(7, &wire)])]);

        let frame = parse_full_frame(frame_bytes).unwrap();
        match frame.body {
            FrameBody::Produce(req) => {
                assert_eq!(req.topics.len(), 1);
                assert_eq!(req.topics[0].topic, "orders");
                assert_eq!(req.topics[0].partitions.len(), 1);
                assert_eq!(req.topics[0].partitions[0].index, 7);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn parses_multiple_topics() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(
            1,
            None,
            0,
            0,
            0,
            &[
                (b"topic-a", &[(0, wire.as_slice())]),
                (b"topic-b", &[(1, wire.as_slice())]),
            ],
        );

        let frame = parse_full_frame(frame_bytes).unwrap();
        match frame.body {
            FrameBody::Produce(req) => {
                assert_eq!(req.topics.len(), 2);
                assert_eq!(req.topics[0].topic, "topic-a");
                assert_eq!(req.topics[1].topic, "topic-b");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn parses_multiple_partitions_in_topic() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(
            1,
            None,
            0,
            0,
            0,
            &[(b"t", &[(0, wire.as_slice()), (1, wire.as_slice())])],
        );

        let frame = parse_full_frame(frame_bytes).unwrap();
        match frame.body {
            FrameBody::Produce(req) => {
                assert_eq!(req.topics[0].partitions.len(), 2);
                assert_eq!(req.topics[0].partitions[0].index, 0);
                assert_eq!(req.topics[0].partitions[1].index, 1);
            }
            _ => panic!(),
        }
    }

    // Two partitions with distinct records — verifies buf cursor advances past first partition's
    // batch bytes so second partition decodes its own data, not a re-read of the first.
    #[test]
    fn multiple_partitions_each_decode_own_records() {
        let b0 = make_batch(0, &[(b"key-p0", b"val-p0")]);
        let b1 = make_batch(0, &[(b"key-p1", b"val-p1")]);
        let wire0 = encode_batch_for_wire(&b0);
        let wire1 = encode_batch_for_wire(&b1);
        let frame_bytes = build_frame(
            1,
            None,
            0,
            0,
            0,
            &[(b"t", &[(0, wire0.as_slice()), (1, wire1.as_slice())])],
        );

        let frame = parse_full_frame(frame_bytes).unwrap();
        match frame.body {
            FrameBody::Produce(req) => {
                let parts = &req.topics[0].partitions;
                assert_eq!(parts.len(), 2);

                let (r0, _) = Record::decode_raw(&parts[0].records.records).unwrap();
                assert_eq!(r0.key, b"key-p0", "partition 0 decoded wrong record");

                let (r1, _) = Record::decode_raw(&parts[1].records.records).unwrap();
                assert_eq!(
                    r1.key, b"key-p1",
                    "partition 1 decoded wrong record — buf cursor not advanced"
                );
            }
            _ => panic!(),
        }
    }

    // --- batch content round-trip ---

    #[test]
    fn batch_fields_survive_codec_roundtrip() {
        let batch = make_batch(42, &[(b"key", b"val")]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(1, None, 0, 0, 0, &[(b"t", &[(0, &wire)])]);

        let frame = parse_full_frame(frame_bytes).unwrap();
        match frame.body {
            FrameBody::Produce(req) => {
                let decoded = &req.topics[0].partitions[0].records;
                assert_eq!(decoded.base_offset, 42);
                assert_eq!(decoded.records_count, 1);

                let (record, _) = Record::decode_raw(&decoded.records).unwrap();
                assert_eq!(record.key, b"key");
                assert_eq!(record.value, b"val");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn batch_with_multiple_records_roundtrip() {
        let batch = make_batch(0, &[(b"k0", b"v0"), (b"k1", b"v1"), (b"k2", b"v2")]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(1, None, 0, 0, 0, &[(b"t", &[(0, &wire)])]);

        let frame = parse_full_frame(frame_bytes).unwrap();
        match frame.body {
            FrameBody::Produce(req) => {
                let decoded = &req.topics[0].partitions[0].records;
                assert_eq!(decoded.records_count, 3);

                let expected_keys: &[&[u8]] = &[b"k0", b"k1", b"k2"];
                let mut pos = 0;
                for expected_key in expected_keys {
                    let (r, consumed) = Record::decode_raw(&decoded.records[pos..]).unwrap();
                    assert_eq!(r.key.as_slice(), *expected_key);
                    pos += consumed;
                }
            }
            _ => panic!(),
        }
    }

    fn build_fetch_frame(
        correlation_id: u32,
        replica_id: i32,
        max_bytes: u32,
        topics: &[(&[u8], &[(u32, u64, u32)])], // (name, [(partition, fetch_offset, max_bytes)])
    ) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32(0); // size placeholder
        buf.put_u32(1); // ApiKey::Fetch = 1
        buf.put_u32(0); // version
        buf.put_u32(correlation_id);
        buf.put_i16(-1); // no client_id

        buf.put_i32(replica_id);
        buf.put_u32(max_bytes);
        buf.put_u32(topics.len() as u32);
        for (name, partitions) in topics {
            buf.put_u16(name.len() as u16);
            buf.extend_from_slice(name);
            buf.put_u32(partitions.len() as u32);
            for (partition, fetch_offset, part_max_bytes) in *partitions {
                buf.put_u32(*partition);
                buf.put_u64(*fetch_offset);
                buf.put_u32(*part_max_bytes);
                buf.put_u64(0); // high_watermark
            }
        }

        let size = (buf.len() - 4) as u32;
        buf[..4].copy_from_slice(&size.to_be_bytes());
        buf.freeze()
    }

    fn build_metadata_frame(topics: &[&[u8]], allow_auto: bool) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32(0); // size placeholder
        buf.put_u32(3); // ApiKey::Metadata = 3
        buf.put_u32(0); // version
        buf.put_u32(1); // correlation_id
        buf.put_i16(-1); // no client_id

        buf.put_u32(topics.len() as u32);
        for name in topics {
            buf.put_i16(name.len() as i16);
            buf.extend_from_slice(name);
        }
        buf.put_u8(allow_auto as u8);

        let size = (buf.len() - 4) as u32;
        buf[..4].copy_from_slice(&size.to_be_bytes());
        buf.freeze()
    }

    // --- fetch parsing ---

    #[test]
    fn parses_fetch_header_and_fields() {
        let bytes = build_fetch_frame(77, -1, 65536, &[(b"events", &[(2, 100, 4096)])]);
        let frame = parse_full_frame(bytes).unwrap();

        assert_eq!(frame.header.correlation_id, 77);
        assert_eq!(frame.header.api_key, ApiKey::Fetch);
        match frame.body {
            FrameBody::Fetch(req) => {
                assert_eq!(req.replica_id, -1);
                assert_eq!(req.max_bytes, 65536);
                assert_eq!(req.topics.len(), 1);
                assert_eq!(req.topics[0].topic, "events");
                assert_eq!(req.topics[0].partitions[0].partition, 2);
                assert_eq!(req.topics[0].partitions[0].fetch_offset, 100);
                assert_eq!(req.topics[0].partitions[0].partition_max_bytes, 4096);
            }
            _ => panic!("expected Fetch"),
        }
    }

    #[test]
    fn parses_fetch_multiple_topics_and_partitions() {
        let bytes = build_fetch_frame(
            1,
            0,
            1024,
            &[
                (b"topic-a", &[(0, 0, 512), (1, 10, 512)]),
                (b"topic-b", &[(0, 5, 256)]),
            ],
        );
        let frame = parse_full_frame(bytes).unwrap();
        match frame.body {
            FrameBody::Fetch(req) => {
                assert_eq!(req.topics.len(), 2);
                assert_eq!(req.topics[0].topic, "topic-a");
                assert_eq!(req.topics[0].partitions.len(), 2);
                assert_eq!(req.topics[0].partitions[1].fetch_offset, 10);
                assert_eq!(req.topics[1].topic, "topic-b");
            }
            _ => panic!(),
        }
    }

    // --- metadata parsing ---

    #[test]
    fn parses_metadata_topics() {
        let bytes = build_metadata_frame(&[b"orders", b"events"], false);
        let frame = parse_full_frame(bytes).unwrap();
        assert_eq!(frame.header.api_key, ApiKey::Metadata);
        match frame.body {
            FrameBody::Metadata(req) => {
                assert_eq!(req.topics, vec!["orders", "events"]);
                assert!(!req.allow_auto_topic_creation);
            }
            _ => panic!("expected Metadata"),
        }
    }

    #[test]
    fn parses_metadata_allow_auto_creation() {
        let bytes = build_metadata_frame(&[], true);
        let frame = parse_full_frame(bytes).unwrap();
        match frame.body {
            FrameBody::Metadata(req) => {
                assert!(req.allow_auto_topic_creation);
                assert!(req.topics.is_empty());
            }
            _ => panic!(),
        }
    }

    // Regression: parses the full original test fixture
    #[test]
    fn parses_full_frame_regression() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(
            42,
            Some(b"client"),
            123,
            1,
            5000,
            &[(b"orders", &[(3, &wire)])],
        );

        let frame = parse_full_frame(frame_bytes).unwrap();
        assert_eq!(frame.header.correlation_id, 42);
        assert_eq!(frame.header.client_id.as_deref(), Some("client"));

        match frame.body {
            FrameBody::Produce(req) => {
                assert_eq!(req.transactional_id, 123);
                assert_eq!(req.acks, Acks::Leader);
                assert_eq!(req.topics.len(), 1);
                assert_eq!(req.topics[0].topic, "orders");
                assert_eq!(req.topics[0].partitions[0].index, 3);
            }
            _ => panic!("expected produce frame"),
        }
    }
}
