use bytes::{BufMut, Bytes, BytesMut};

use crate::protocol::{
    body::FrameBody,
    codec::{ParseError, RequestDecoder},
    header::RequestHeader,
};

pub mod body;
pub mod codec;
pub mod fetch;
pub mod header;
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

        buf.put_u32(self.size);

        buf.put_u32(u32::from(self.header.api_key));
        buf.put_u32(self.header.api_version);
        buf.put_u32(self.header.correlation_id);

        if let Some(client_id) = &self.header.client_id {
            buf.put_u16(client_id.len() as u16);
            buf.copy_from_slice(client_id.as_bytes());
        }

        match &self.body {
            FrameBody::Produce(req) => {
                buf.put_u64(req.transactional_id);
                buf.put_u32(u32::from(req.acks));

                // FIXME: should be millis instead of secs
                buf.put_u64(req.timeout.as_secs());

                buf.put_u16(req.topics.len() as u16);
                for t in &req.topics {
                    buf.put_u16(t.topic.len() as u16);
                    buf.copy_from_slice(t.topic.as_bytes());

                    // FIXME: bug in reading and writing size, id and length
                    buf.put_u32(t.partitions.len() as u32);
                    for p in &t.partitions {
                        buf.put_u16(p.index as u16);
                        buf.copy_from_slice(&p.records);
                    }
                }
            }
            FrameBody::FetchResponse(_) => todo!(),
            FrameBody::ProduceResponse => todo!(),
            FrameBody::Fetch(_) => todo!(),
        }

        buf.freeze()
    }

    pub fn decode(buf: Bytes) -> Result<Self, ParseError> {
        let mut decoder = RequestDecoder;
        decoder.parse(buf)
    }
}
