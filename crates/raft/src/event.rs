use crate::rpc::{
    BeginQuorumEpochRequest, EndQuorumEpochRequest, FetchRequest, FetchResponse, VoteRequest,
    VoteResponse,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    ElectionTimeout,
    HeartbeatTimeout,
    VoteRequest { from: u16, req: VoteRequest },
    VoteResponse { from: u16, resp: VoteResponse },
    BeginQuorumEpoch { from: u16, req: BeginQuorumEpochRequest },
    EndQuorumEpoch { from: u16, req: EndQuorumEpochRequest },
    FetchRequest { from: u16, req: FetchRequest },
    FetchResponse { from: u16, resp: FetchResponse },
    Propose { data: Vec<u8>, propose_id: u64 },
}
