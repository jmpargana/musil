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
                let topics_size: u32 = req
                    .topics
                    .iter()
                    .map(|t| {
                        2 + t.topic.len() as u32
                            + 4
                            + t.partitions
                                .iter()
                                .map(|p| 2 + 4 + p.records.get_size())
                                .sum::<u32>()
                    })
                    .sum();
                8 + 4 + 8 + 2 + topics_size
            }
            FrameBody::ProduceResponse(produce_response) => produce_response.get_size(),
            FrameBody::Fetch(req) => {
                let topics_size: u32 = req
                    .topics
                    .iter()
                    .map(|t| 2 + t.topic.len() as u32 + 4 + t.partitions.len() as u32 * 24)
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

impl From<ProduceRequest> for FrameBody {
    fn from(value: ProduceRequest) -> Self {
        FrameBody::Produce(value)
    }
}

impl From<ProduceResponse> for FrameBody {
    fn from(value: ProduceResponse) -> Self {
        FrameBody::ProduceResponse(value)
    }
}

impl From<FetchRequest> for FrameBody {
    fn from(value: FetchRequest) -> Self {
        FrameBody::Fetch(value)
    }
}

impl From<FetchResponse> for FrameBody {
    fn from(value: FetchResponse) -> Self {
        FrameBody::FetchResponse(value)
    }
}

impl From<MetadataRequest> for FrameBody {
    fn from(value: MetadataRequest) -> Self {
        FrameBody::Metadata(value)
    }
}

impl From<MetadataResponse> for FrameBody {
    fn from(value: MetadataResponse) -> Self {
        FrameBody::MetadataResponse(value)
    }
}

impl From<CreateTopicRequest> for FrameBody {
    fn from(value: CreateTopicRequest) -> Self {
        FrameBody::Topic(value)
    }
}

impl From<CreateTopicResponse> for FrameBody {
    fn from(value: CreateTopicResponse) -> Self {
        FrameBody::TopicResponse(value)
    }
}
