use std::time::Duration;

use bytes::{Buf, Bytes};
use proto::{error::ProtoError, record_batch::RecordBatch};

use super::{body::FrameBody, error_codes::ErrorCode};
use crate::protocol::{
    Frame,
    fetch::{
        request::{
            fetch_partition::FetchPartition, fetch_request::FetchRequest, fetch_topic::FetchTopic,
        },
        response::{
            fetch_response::FetchResponse, partition_response::PartitionResponse,
            topic_response::TopicResponse,
        },
    },
    header::{ApiKey, RequestHeader},
    metadata::{
        BrokerMetadata, CreateTopicRequest, CreateTopicResponse, MetadataRequest, MetadataResponse,
        PartitionMetadata, TopicMetadata, TopicPartitonAssignment, TopicRequest,
        TopicResponse as CreateTopicTopicResponse,
    },
    produce::{
        request::{
            produce_partition::ProducePartition, produce_request::ProduceRequest,
            produce_topic::ProduceTopic,
        },
        response::{
            partition_response::{CurrentLeader, ProducePartitionResponse},
            produce_response::ProduceResponse,
            topic_response::ProduceTopicResponse,
        },
    },
};

#[derive(Debug)]
pub struct RequestDecoder;

#[derive(Debug)]
pub enum ParseError {
    InvalidApiKey,
    InvalidAck,
    InvalidClientId,
    InvalidBatch(ProtoError),
}

impl RequestDecoder {
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

        let body: FrameBody = match api_key {
            ApiKey::Produce => self.parse_produce(buf)?,
            ApiKey::Fetch => self.parse_fetch(buf)?,
            ApiKey::Metadata => self.parse_metadata(buf)?,
            ApiKey::CreateTopics => self.parse_create_topics(buf)?,
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
            let bytes = buf.split_to(topic_name_length as usize);
            let topic = String::from_utf8_lossy(&bytes).to_string();

            let partition_length = buf.get_u32();
            let mut partitions = Vec::new();
            for _ in 0..partition_length {
                let partition_id = buf.get_u16();
                let _batch_len = buf.get_u32();
                let batch_bytes = buf.split_to(_batch_len as usize);
                let record_batch =
                    RecordBatch::decode(batch_bytes).map_err(ParseError::InvalidBatch)?;

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
                });
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
            topics.push(String::from_utf8_lossy(&topic).to_string());
        }
        let allow_auto_topic_creation = buf.get_u8() != 0;
        Ok(FrameBody::Metadata(MetadataRequest {
            allow_auto_topic_creation,
            topics,
        }))
    }

    fn parse_create_topics(&self, buf: &mut Bytes) -> Result<FrameBody, ParseError> {
        let topics_count = buf.get_u16();
        let mut topics = Vec::new();
        for _ in 0..topics_count {
            let name_len = buf.get_u16();
            let name = String::from_utf8_lossy(&buf.split_to(name_len as usize)).to_string();
            let num_partitions = buf.get_i32();
            let replication_factor = buf.get_u16();
            let assignments_count = buf.get_u16();
            let mut assignments = Vec::new();
            for _ in 0..assignments_count {
                let partition_index = buf.get_i32();
                let broker_ids = buf.get_i32();
                assignments.push(TopicPartitonAssignment {
                    partition_index,
                    broker_ids,
                });
            }
            topics.push(TopicRequest {
                name,
                num_partitions,
                replication_factor,
                assignments,
            });
        }
        let timeout_ms = buf.get_u32();
        let validate_only = buf.get_u8() != 0;
        Ok(FrameBody::Topic(CreateTopicRequest {
            topics,
            timeout_ms,
            validate_only,
        }))
    }
}

#[derive(Debug)]
pub struct ResponseDecoder;

impl ResponseDecoder {
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

        let body = match api_key {
            ApiKey::Metadata => self.parse_metadata_response(buf)?,
            ApiKey::Produce => self.parse_produce_response(buf)?,
            ApiKey::Fetch => self.parse_fetch_response(buf)?,
            ApiKey::CreateTopics => self.parse_create_topics_response(buf)?,
        };

        Ok(Frame { size, header, body })
    }

    fn parse_metadata_response(&self, buf: &mut Bytes) -> Result<FrameBody, ParseError> {
        let throttle_time_ms = buf.get_u32();
        let brokers_count = buf.get_u32();
        let mut brokers = Vec::new();
        for _ in 0..brokers_count {
            let node_id = buf.get_i32();
            let host_len = buf.get_u16();
            let host = String::from_utf8_lossy(&buf.split_to(host_len as usize)).to_string();
            let port = buf.get_i32();
            brokers.push(BrokerMetadata {
                node_id,
                host,
                port,
            });
        }
        let controller_id = buf.get_i32();
        let topics_count = buf.get_u32();
        let mut topics = Vec::new();
        for _ in 0..topics_count {
            let error_code =
                ErrorCode::try_from(buf.get_i16()).unwrap_or(ErrorCode::UnknownServerError);
            let name_len = buf.get_u16();
            let name = String::from_utf8_lossy(&buf.split_to(name_len as usize)).to_string();
            let partitions_count = buf.get_u32();
            let mut partitions = Vec::new();
            for _ in 0..partitions_count {
                let p_error_code =
                    ErrorCode::try_from(buf.get_i16()).unwrap_or(ErrorCode::UnknownServerError);
                let partition_index = buf.get_i32();
                let leader_id = buf.get_i32();
                let replica_nodes = buf.get_u32();
                let isr_nodes = buf.get_u32();
                let offline_replicas = buf.get_u32();
                partitions.push(PartitionMetadata {
                    error_code: p_error_code,
                    partition_index,
                    leader_id,
                    replica_nodes,
                    isr_nodes,
                    offline_replicas,
                });
            }
            topics.push(TopicMetadata {
                error_code,
                name,
                partitions,
            });
        }
        let error_code =
            ErrorCode::try_from(buf.get_i16()).unwrap_or(ErrorCode::UnknownServerError);
        Ok(FrameBody::MetadataResponse(MetadataResponse {
            throttle_time_ms,
            brokers,
            controller_id,
            topics,
            error_code,
        }))
    }

    fn parse_produce_response(&self, buf: &mut Bytes) -> Result<FrameBody, ParseError> {
        let throttle_time_ms = buf.get_u32();
        let responses_count = buf.get_u32();
        let mut responses = Vec::new();
        for _ in 0..responses_count {
            let topic_len = buf.get_u16();
            let topic = String::from_utf8_lossy(&buf.split_to(topic_len as usize)).to_string();
            let partition_count = buf.get_u32();
            let mut partition_responses = Vec::new();
            for _ in 0..partition_count {
                let index = buf.get_u32();
                let error_code =
                    ErrorCode::try_from(buf.get_i16()).unwrap_or(ErrorCode::UnknownServerError);
                let base_offset = buf.get_u64();
                let log_append_time_ms = buf.get_u64();
                let log_start_offset = buf.get_u64();
                let error_message_len = buf.get_i16();
                let error_message = if error_message_len > 0 {
                    String::from_utf8_lossy(&buf.split_to(error_message_len as usize)).to_string()
                } else {
                    String::new()
                };
                let has_leader = buf.get_u8() != 0;
                let current_leader = if has_leader {
                    let leader_id = buf.get_i32();
                    let leader_epoch = buf.get_u32();
                    Some(CurrentLeader {
                        leader_id,
                        leader_epoch,
                    })
                } else {
                    None
                };
                partition_responses.push(ProducePartitionResponse {
                    index,
                    error_code,
                    base_offset,
                    log_append_time_ms,
                    log_start_offset,
                    error_message,
                    current_leader,
                });
            }
            responses.push(ProduceTopicResponse {
                topic,
                partition_responses,
            });
        }
        Ok(FrameBody::ProduceResponse(ProduceResponse {
            throttle_time_ms,
            responses,
        }))
    }

    fn parse_fetch_response(&self, buf: &mut Bytes) -> Result<FrameBody, ParseError> {
        let throttle_time_ms = buf.get_u32();
        let responses_count = buf.get_u32();
        let mut responses = Vec::new();
        for _ in 0..responses_count {
            let topic_len = buf.get_u16();
            let topic = String::from_utf8_lossy(&buf.split_to(topic_len as usize)).to_string();
            let partitions_count = buf.get_u32();
            let mut partitions = Vec::new();
            for _ in 0..partitions_count {
                let partition_index = buf.get_u32();
                let error_code =
                    ErrorCode::try_from(buf.get_i16()).unwrap_or(ErrorCode::UnknownServerError);
                let high_watermark = buf.get_u64();
                let log_start_offset = buf.get_u64();
                let records_count = buf.get_u32();
                let mut records = Vec::new();
                for _ in 0..records_count {
                    let base_offset = buf.get_u64();
                    let batch_length = buf.get_u32();
                    let payload = buf.split_to(batch_length as usize);
                    let mut combined = Vec::with_capacity(12 + batch_length as usize);
                    combined.extend_from_slice(&base_offset.to_be_bytes());
                    combined.extend_from_slice(&batch_length.to_be_bytes());
                    combined.extend_from_slice(&payload);
                    records.push(
                        RecordBatch::decode(Bytes::from(combined))
                            .map_err(ParseError::InvalidBatch)?,
                    );
                }
                partitions.push(PartitionResponse {
                    partition_index,
                    error_code,
                    high_watermark,
                    log_start_offset,
                    records,
                });
            }
            responses.push(TopicResponse { topic, partitions });
        }
        Ok(FrameBody::FetchResponse(FetchResponse {
            throttle_time_ms,
            responses,
        }))
    }

    fn parse_create_topics_response(&self, buf: &mut Bytes) -> Result<FrameBody, ParseError> {
        let throttle_time_ms = buf.get_u32();
        let topics_count = buf.get_u16();
        let mut topics = Vec::new();
        for _ in 0..topics_count {
            let name_len = buf.get_u16();
            let name = String::from_utf8_lossy(&buf.split_to(name_len as usize)).to_string();
            let error_code =
                ErrorCode::try_from(buf.get_i16()).unwrap_or(ErrorCode::UnknownServerError);
            let error_message_len = buf.get_u16();
            let error_message =
                String::from_utf8_lossy(&buf.split_to(error_message_len as usize)).to_string();
            let num_partitions = buf.get_i32();
            let replication_factor = buf.get_u16();
            topics.push(CreateTopicTopicResponse {
                name,
                error_code,
                error_message,
                num_partitions,
                replication_factor,
            });
        }
        Ok(FrameBody::TopicResponse(CreateTopicResponse {
            throttle_time_ms,
            topics,
        }))
    }
}
