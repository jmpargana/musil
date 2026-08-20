use crate::log::LogEntry;
use crate::rpc::{
    BeginQuorumEpochRequest, EndQuorumEpochRequest, FetchRequest, FetchResponse, VoteRequest,
    VoteResponse,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    // Persistence (execute first, fsync)
    PersistQuorumState,
    AppendToLog(LogEntry),
    TruncateLog(u64),

    // Network (execute after persists)
    SendVote(u16, VoteRequest),
    SendVoteResponse(u16, VoteResponse),
    SendBeginQuorumEpoch(Vec<u16>, BeginQuorumEpochRequest),
    SendEndQuorumEpoch(Vec<u16>, EndQuorumEpochRequest),
    SendFetchResponse(u16, FetchResponse),
    SendFetchRequest(u16, FetchRequest),

    // Timers
    ResetElectionTimer,
    ResetHeartbeatTimer,

    // Client notifications (Runner routes by propose_id)
    CommitPropose(u64),
    RejectPropose(u64),
}
