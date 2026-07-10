use std::time::Duration;

use bytes::{Buf, Bytes};

use crate::message::{
    Message,
    body::{MessageBody, ProduceRequest},
    header::{MessageApiKey, MessageHeader},
    produce::{ProducePartition, ProduceTopic},
};

// MessageParser doesn't own buffer, instead it consumes just enough to find the size and then creates an event with fd ptr and size
#[derive(Debug)]
pub struct MessageParser;

#[derive(Debug)]
pub enum ParseError {
    InvalidApiKey,
    InvalidAck,
    InvalidClientId,
}

impl MessageParser {
    pub fn parse(&mut self, mut buf: Bytes) -> Result<Message, ParseError> {
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

        let api_key: MessageApiKey = api_key.try_into().map_err(|_| ParseError::InvalidApiKey)?;

        let header = MessageHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        };

        // TODO: depending on message type we need to read different values from body
        let body: MessageBody = match api_key {
            // FIXME: before doing this I need to copy more bytes on demand to keep reading
            MessageApiKey::Produce => self.parse_produce(buf)?,
            MessageApiKey::Fetch => {
                todo!()
            }
        };

        Ok(Message { size, header, body })
    }

    fn parse_produce(&self, mut buf: Bytes) -> Result<MessageBody, ParseError> {
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
                let batch = buf.split_to(batch_len);

                partitions.push(ProducePartition {
                    partition_id,
                    batch,
                });
            }

            topics.push(ProduceTopic { topic, partitions });
        }

        Ok(MessageBody::Produce(ProduceRequest {
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

    use crate::message::ack::Ack;

    use super::*;

    // TODO: refactor to use encoder, which will be needed before writing to network
    fn produce_message_bytes() -> Bytes {
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
    fn parses_full_message() {
        let bytes = produce_message_bytes();
        let mut parser = MessageParser;
        let message = parser.parse(bytes).unwrap();

        assert_eq!(message.size, 65);
        assert_eq!(message.header.api_key, MessageApiKey::Produce);
        assert_eq!(message.header.api_version, 0);
        assert_eq!(message.header.correlation_id, 42);
        assert_eq!(message.header.client_id.as_deref(), Some("client"));

        match message.body {
            MessageBody::Produce(ProduceRequest {
                transactional_id,
                acks,
                timeout: _,
                topics,
            }) => {
                assert_eq!(transactional_id, 123);
                assert_eq!(acks, Ack::Leader);
                assert_eq!(topics.len(), 1);
                assert_eq!(topics[0].topic, "orders");
                assert_eq!(topics[0].partitions.len(), 1);
                assert_eq!(topics[0].partitions[0].partition_id, 3);
                assert_eq!(topics[0].partitions[0].batch.as_ref(), b"hello");
            }
            _ => panic!("expected produce message"),
        }
    }
}
