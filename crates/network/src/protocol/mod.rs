use bytes::{BufMut, Bytes, BytesMut};

use crate::protocol::{
    body::FrameBody,
    codec::{ParseError, RequestDecoder, ResponseDecoder},
    header::{ApiKey, RequestHeader},
    produce::request::produce_request::ProduceRequest,
};

use rand::Rng;

pub mod body;
pub mod codec;
pub mod error_codes;
pub mod fetch;
pub mod header;
pub mod metadata;
pub mod produce;

// TODO: maybe this should be an enum so that decode becomes a From and works for both types
#[derive(Debug)]
pub struct Frame {
    pub size: u32,
    pub header: RequestHeader,
    pub body: FrameBody,
}

impl From<ProduceRequest> for Frame {
    fn from(value: ProduceRequest) -> Self {
        Frame::new(ApiKey::Produce, value.into())
    }
}

impl Frame {
    pub fn new(api_key: ApiKey, body: FrameBody) -> Self {
        let mut rng = rand::rng();
        let correlation_id: u32 = rng.random::<u32>();

        let header = RequestHeader {
            api_key,
            api_version: 0,
            correlation_id,
            client_id: None,
        };

        let size = header.get_size() + body.get_size();

        Self { size, header, body }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_u32(0);

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

        // TODO: should this be a From<Bytes> for each of the types instead?
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
            FrameBody::Topic(req) => {
                buf.put_u16(req.topics.len() as u16);
                for t in &req.topics {
                    buf.put_u16(t.name.len() as u16);
                    buf.put_slice(t.name.as_bytes());
                    buf.put_i32(t.num_partitions);
                    buf.put_u16(t.replication_factor);
                    buf.put_u16(t.assignments.len() as u16);
                    for a in &t.assignments {
                        buf.put_i32(a.partition_index);
                        buf.put_i32(a.broker_ids);
                    }
                }
                buf.put_u32(req.timeout_ms);
                buf.put_u8(req.validate_only as u8);
            }
            FrameBody::TopicResponse(resp) => {
                buf.put_u32(resp.throttle_time_ms);
                buf.put_u16(resp.topics.len() as u16);
                for t in &resp.topics {
                    buf.put_u16(t.name.len() as u16);
                    buf.put_slice(t.name.as_bytes());
                    buf.put_i16(i16::from(t.error_code));
                    buf.put_u16(t.error_message.len() as u16);
                    buf.put_slice(t.error_message.as_bytes());
                    buf.put_i32(t.num_partitions);
                    buf.put_u16(t.replication_factor);
                }
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

    pub fn decode_response(buf: &Bytes, size: u32) -> Result<Self, ParseError> {
        let mut decoder = ResponseDecoder;
        let mut buf = buf.clone();
        decoder.parse(&mut buf, size)
    }
}
