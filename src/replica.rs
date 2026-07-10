use std::time::Instant;

#[derive(Clone)]
pub struct ReplicaMetadata {
    // TODO: maybe this field get's removed?
    pub broker_id: String,
    pub replica_id: u32,
    pub log_end_offset: u64,

    // both can be refactored to single ReplicaStatus
    is_in_sync: bool,
    last_heartbeat: Instant,
    // TODO: channel to upload
}

impl ReplicaMetadata {
    pub fn empty(broker_id: String, replica_id: u32) -> Self {
        Self {
            broker_id,
            replica_id,
            log_end_offset: 0,
            // starts off working
            is_in_sync: true,
            last_heartbeat: Instant::now(),
        }
    }
}
