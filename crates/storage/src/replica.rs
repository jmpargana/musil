use std::time::Instant;

#[derive(Clone, Debug)]
pub struct ReplicaMetadata {
    pub broker_id: String,
    pub replica_id: u32,
    pub log_end_offset: u64,

    pub is_in_sync: bool,
    _last_heartbeat: Instant,
}

impl ReplicaMetadata {
    pub fn empty(broker_id: String, replica_id: u32) -> Self {
        Self {
            broker_id,
            replica_id,
            log_end_offset: 0,
            is_in_sync: true,
            _last_heartbeat: Instant::now(),
        }
    }
}
