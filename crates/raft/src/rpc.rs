use crate::log::LogEntry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRequest {
    pub epoch: u32,
    pub candidate_id: u16,
    pub last_log_epoch: u32,
    pub last_log_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteResponse {
    pub epoch: u32,
    pub granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    pub epoch: u32,
    pub fetch_offset: u64,
    pub last_fetched_epoch: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponse {
    pub epoch: u32,
    pub high_watermark: u64,
    pub entries: Vec<LogEntry>,
    pub diverging: Option<Diverging>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diverging {
    pub epoch: u32,
    pub end_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginQuorumEpochRequest {
    pub epoch: u32,
    pub leader_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndQuorumEpochRequest {
    pub epoch: u32,
    pub leader_id: u16,
    pub voters: Vec<ReplicaState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaState {
    pub id: u16,
    pub log_end_offset: u64,
}
