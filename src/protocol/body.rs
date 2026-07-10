use crate::protocol::{
    fetch::{request::fetch_request::FetchRequest, response::fetch_response::FetchResponse},
    produce::request::produce_request::ProduceRequest,
};

pub enum FrameBody {
    Produce(ProduceRequest),
    ProduceResponse,
    Fetch(FetchRequest),
    FetchResponse(FetchResponse),
}
