use std::time::Duration;

use bytes::{Buf, Bytes};

use crate::{
    protocol::{
        Frame,
        header::{ApiKey, RequestHeader},
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
    pub fn parse(&mut self, mut buf: Bytes) -> Result<Frame, ParseError> {
        let size = buf.get_u32();

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
            // FIXME: before doing this I need to copy more bytes on demand to keep reading
            ApiKey::Produce => self.parse_produce(buf)?,
            ApiKey::Fetch => {
                todo!()
            }
        };

        Ok(Frame { size, header, body })
    }

    fn parse_produce(&self, mut buf: Bytes) -> Result<FrameBody, ParseError> {
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
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use crate::protocol::{
        body::FrameBody,
        codec::RequestDecoder,
        header::ApiKey,
        produce::{acks::Acks, request::produce_request::ProduceRequest},
    };
    use crate::storage::record::Record;
    use crate::storage::record_batch::RecordBatch;

    use super::*;

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

        let frame = RequestDecoder.parse(frame_bytes).unwrap();

        assert_eq!(frame.header.api_key, ApiKey::Produce);
        assert_eq!(frame.header.correlation_id, 42);
        assert_eq!(frame.header.client_id.as_deref(), Some("client"));
    }

    #[test]
    fn parses_null_client_id() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(1, None, 0, 0, 0, &[(b"t", &[(0, &wire)])]);

        let frame = RequestDecoder.parse(frame_bytes).unwrap();
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

        let result = RequestDecoder.parse(buf.freeze());
        assert!(matches!(result, Err(ParseError::InvalidApiKey)));
    }

    #[test]
    fn rejects_invalid_ack_value() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(1, None, 0, 99, 0, &[(b"t", &[(0, &wire)])]); // acks=99 invalid

        let result = RequestDecoder.parse(frame_bytes);
        assert!(matches!(result, Err(ParseError::InvalidAck)));
    }

    // --- produce body parsing ---

    #[test]
    fn parses_produce_body_fields() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(1, None, 0xDEAD, 1, 5000, &[(b"orders", &[(3, &wire)])]);

        let frame = RequestDecoder.parse(frame_bytes).unwrap();
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
        let frame = RequestDecoder.parse(frame_bytes).unwrap();
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
        let frame = RequestDecoder.parse(frame_bytes).unwrap();
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

        let frame = RequestDecoder.parse(frame_bytes).unwrap();
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
            1, None, 0, 0, 0,
            &[
                (b"topic-a", &[(0, wire.as_slice())]),
                (b"topic-b", &[(1, wire.as_slice())]),
            ],
        );

        let frame = RequestDecoder.parse(frame_bytes).unwrap();
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
            1, None, 0, 0, 0,
            &[(b"t", &[(0, wire.as_slice()), (1, wire.as_slice())])],
        );

        let frame = RequestDecoder.parse(frame_bytes).unwrap();
        match frame.body {
            FrameBody::Produce(req) => {
                assert_eq!(req.topics[0].partitions.len(), 2);
                assert_eq!(req.topics[0].partitions[0].index, 0);
                assert_eq!(req.topics[0].partitions[1].index, 1);
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

        let frame = RequestDecoder.parse(frame_bytes).unwrap();
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

        let frame = RequestDecoder.parse(frame_bytes).unwrap();
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

    // Regression: parses the full original test fixture
    #[test]
    fn parses_full_frame_regression() {
        let batch = make_batch(0, &[]);
        let wire = encode_batch_for_wire(&batch);
        let frame_bytes = build_frame(42, Some(b"client"), 123, 1, 5000, &[(b"orders", &[(3, &wire)])]);

        let frame = RequestDecoder.parse(frame_bytes).unwrap();
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
