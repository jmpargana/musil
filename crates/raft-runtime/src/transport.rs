use async_trait::async_trait;
use raft::{
    BeginQuorumEpochRequest, EndQuorumEpochRequest, Event, FetchRequest, FetchResponse,
    VoteRequest, VoteResponse,
};
use tokio::sync::oneshot;

use crate::runner::ProposeResult;

#[derive(Debug)]
pub enum RunnerInput {
    NetworkEvent(Event),
    Propose {
        data: Vec<u8>,
        reply: oneshot::Sender<ProposeResult>,
    },
}

#[derive(Debug, Clone)]
pub enum RaftMessage {
    Vote(VoteRequest),
    VoteResponse(VoteResponse),
    BeginQuorumEpoch(BeginQuorumEpochRequest),
    EndQuorumEpoch(EndQuorumEpochRequest),
    FetchRequest(FetchRequest),
    FetchResponse(FetchResponse),
}

#[async_trait]
pub trait Transport: Send {
    async fn recv(&mut self) -> RunnerInput;
    async fn send(&mut self, to: u16, message: RaftMessage);
}
