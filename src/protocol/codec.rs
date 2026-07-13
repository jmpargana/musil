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

                // FIXME: this is most likely wrong. We need to keep iterating, so position needs to change.
                // Instead we should use Bytes.
                // I need to figure out if you pass a reference, if you pass Bytes (because it has a reference)
                // Or if I return Bytes back to reassign, like a move + move.
                let record_batch = RecordBatch::decode(&buf, 0);

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
    use bytes::{BufMut, BytesMut};

    use crate::protocol::{
        body::FrameBody,
        codec::RequestDecoder,
        header::ApiKey,
        produce::{acks::Acks, request::produce_request::ProduceRequest},
    };

    use super::*;

    // TODO: refactor to use encoder, which will be needed before writing to network
    fn produce_frame_bytes() -> Bytes {
        let mut buf = BytesMut::new();

        // size
        buf.put_u32(0); // placeholder

        // header
        buf.put_u32(0); // api key
        buf.put_u32(0); // version
        buf.put_u32(42); // correlation id

        let client = b"client";
        buf.put_u16(client.len() as u16);
        buf.extend_from_slice(client);

        // produce body
        buf.put_u64(123); // transactional id
        buf.put_u32(1); // acks
        buf.put_u64(5000); // timeout

        // topics
        buf.put_u16(1);

        let topic = b"orders";
        buf.put_u16(topic.len() as u16);
        buf.extend_from_slice(topic);

        // partitions
        buf.put_u32(1);

        buf.put_u16(3); // partition id

        let batch = b"hello";
        buf.put_u32(batch.len() as u32);
        buf.extend_from_slice(batch);

        // update size
        let size = (buf.len() - 4) as u32;
        buf[..4].copy_from_slice(&size.to_be_bytes());

        buf.freeze()
    }

    #[test]
    fn parses_full_frame() {
        let bytes = produce_frame_bytes();
        let mut decoder = RequestDecoder;
        let frame = decoder.parse(bytes).unwrap();

        assert_eq!(frame.size, 65);
        assert_eq!(frame.header.api_key, ApiKey::Produce);
        assert_eq!(frame.header.api_version, 0);
        assert_eq!(frame.header.correlation_id, 42);
        assert_eq!(frame.header.client_id.as_deref(), Some("client"));

        match frame.body {
            FrameBody::Produce(ProduceRequest {
                transactional_id,
                acks,
                timeout: _,
                topics,
            }) => {
                assert_eq!(transactional_id, 123);
                assert_eq!(acks, Acks::Leader);
                assert_eq!(topics.len(), 1);
                assert_eq!(topics[0].topic, "orders");
                assert_eq!(topics[0].partitions.len(), 1);
                assert_eq!(topics[0].partitions[0].index, 3);
                // assert_eq!(topics[0].partitions[0].records.as_ref(), b"hello");
            }
            _ => panic!("expected produce frame"),
        }
    }
}
