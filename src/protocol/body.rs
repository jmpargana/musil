use crate::protocol::{
    fetch::{request::fetch_request::FetchRequest, response::fetch_response::FetchResponse},
    metadata::{MetadataRequest, MetadataResponse},
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
}

impl FrameBody {
    pub fn get_size(&self) -> u32 {
        match &self {
            FrameBody::Produce(produce_request) => todo!(),
            FrameBody::ProduceResponse(produce_response) => produce_response.get_size(),
            FrameBody::Fetch(fetch_request) => todo!(),
            FrameBody::FetchResponse(fetch_response) => fetch_response.get_size(),
            FrameBody::Metadata(metadata_request) => metadata_request.get_size(),
            FrameBody::MetadataResponse(metadata_response) => metadata_response.get_size(),
        }
    }
}
