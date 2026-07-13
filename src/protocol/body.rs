use crate::protocol::{
    fetch::{request::fetch_request::FetchRequest, response::fetch_response::FetchResponse},
    produce::{
        request::produce_request::ProduceRequest, response::produce_response::ProduceResponse,
    },
};

pub enum FrameBody {
    Produce(ProduceRequest),
    ProduceResponse(ProduceResponse),
    Fetch(FetchRequest),
    FetchResponse(FetchResponse),
}
