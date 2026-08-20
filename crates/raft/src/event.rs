use broker::metadata_record::MetadataRecord;
use tokio::sync::oneshot;

pub enum RaftEvent {
    VoteRequest {
        req: VoteRequest,
        reply: oneshot::Sender<VoteResponse>,
    },
    VoteResponse {
        from: u16,
        resp: VoteResponse,
    },
    BeginQuorumEpoch {
        req: BeginQuorumEpochRequest,
    },
    EndQuorumEpoch {
        req: EndQuorumEpochRequest,
    },
    FetchRequest {
        req: FetchRequest,
        reply: oneshot::Sender<FetchResponse>,
    },
    FetchResponse {
        from: u16,
        resp: FetchResponse,
    },
    Propose {
        record: MetadataRecord,
        reply: oneshot::Sender<ProposeResult>,
    },
    ElectionTimeout,
    HeartbeatTimeout,
}
