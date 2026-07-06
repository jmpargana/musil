use std::time::Instant;

use tokio::sync::mpsc;

use crate::command::Command;

#[derive(Clone)]
pub struct ReplicaMetadata {
    pub broker_id: String,
    pub log_end_offset: u64,

    // both can be refactored to single ReplicaStatus
    is_in_sync: bool,
    last_heartbeat: Instant,
    // TODO: channel to upload
}

impl ReplicaMetadata {
    pub fn empty(broker_id: String) -> Self {
        Self {
            broker_id,
            log_end_offset: 0,
            // starts off working
            is_in_sync: true,
            last_heartbeat: Instant::now(),
        }
    }
}
