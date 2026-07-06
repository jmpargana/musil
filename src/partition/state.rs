use std::{cmp::max, mem, sync::Arc};

use derive_builder::Builder;
use tokio::sync::mpsc;

use crate::{
    broker,
    record::Record,
    replica::ReplicaMetadata,
    segment::{
        active::ActiveSegment,
        metadata::{RecordLocation, Segment},
    },
};

#[derive(Clone)]
pub struct PartitionState {
    pub segments: Arc<Vec<Arc<Segment>>>,
    pub log_end_offset: u64,
    pub high_watermark: u64,
    pub replicas: Arc<Vec<ReplicaMetadata>>,
}

// TODO: partition should always start with 0
// bug: subtraction overflow
impl PartitionState {
    pub fn new(replicas: Vec<ReplicaMetadata>) -> Self {
        Self {
            segments: Arc::new(vec![]),
            log_end_offset: 0,
            high_watermark: 0,
            replicas: Arc::new(replicas),
        }
    }

    pub fn find_pos(&self, offset: u64) -> Option<RecordLocation> {
        if offset > self.high_watermark {
            return None;
        }
        let idx = self
            .segments
            .partition_point(|segment| segment.base_offset <= offset);
        // FIXME: something feels off about this
        self.segments[idx - 1].find_pos(offset)
    }

    pub fn ack_replica(mut self, broker_id: String, offset: u64) -> Self {
        let replicas = Arc::make_mut(&mut self.replicas);

        if let Some(ref mut replica) = replicas.iter_mut().find(|r| r.broker_id == broker_id) {
            // FIXME: what if an ack for a more recent offset arrived?
            replica.log_end_offset = max(replica.log_end_offset, offset);
        }

        self
    }

    pub fn consume(
        self,
        segments: Arc<Vec<Arc<Segment>>>,
        log_end_offset: u64,
        high_watermark: u64,
    ) -> Self {
        Self {
            segments,
            log_end_offset,
            high_watermark,
            ..self
        }
    }
}
