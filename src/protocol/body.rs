use crate::protocol::{
    fetch::{request::fetch_request::FetchRequest, response::fetch_response::FetchResponse},
    metadata::{CreateTopicRequest, CreateTopicResponse, MetadataRequest, MetadataResponse},
    produce::{
        request::produce_request::ProduceRequest, response::produce_response::ProduceResponse,
    },
};

#[derive(Debug)]
pub enum FrameBody {
    Produce(ProduceRequest),
    ProduceResponse(ProduceResponse),
    Fetch(FetchRequest),
    FetchResponse(FetchResponse),
    Metadata(MetadataRequest),
    MetadataResponse(MetadataResponse),
    Topic(CreateTopicRequest),
    TopicResponse(CreateTopicResponse),
}

impl FrameBody {
    pub fn get_size(&self) -> u32 {
        match &self {
            FrameBody::Produce(req) => {
                // transactional_id(8) + acks(4) + timeout(8) + topics_count(2)
                let topics_size: u32 = req
                    .topics
                    .iter()
                    .map(|t| {
                        // topic_len(2) + topic + partitions_count(4)
                        2 + t.topic.len() as u32
                            + 4
                            + t.partitions
                                .iter()
                                .map(|p| {
                                    // partition_id(2) + batch_wire_len(4) + encode_header(16) + records
                                    2 + 4 + p.records.get_size()
                                })
                                .sum::<u32>()
                    })
                    .sum();
                8 + 4 + 8 + 2 + topics_size
            }
            FrameBody::ProduceResponse(produce_response) => produce_response.get_size(),
            FrameBody::Fetch(req) => {
                // replica_id(4) + max_bytes(4) + topics_count(4)
                let topics_size: u32 = req
                    .topics
                    .iter()
                    .map(|t| {
                        // topic_len(2) + topic + partitions_count(4) + per partition: 4+8+4+8=24
                        2 + t.topic.len() as u32 + 4 + t.partitions.len() as u32 * 24
                    })
                    .sum();
                4 + 4 + 4 + topics_size
            }
            FrameBody::FetchResponse(fetch_response) => fetch_response.get_size(),
            FrameBody::Metadata(metadata_request) => metadata_request.get_size(),
            FrameBody::MetadataResponse(metadata_response) => metadata_response.get_size(),
            FrameBody::Topic(req) => req.get_size(),
            FrameBody::TopicResponse(resp) => resp.get_size(),
        }
    }
}
