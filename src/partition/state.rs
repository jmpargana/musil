use std::{cmp::max, sync::Arc};

use crate::{
    protocol::fetch::{
        request::fetch_partition::FetchPartition,
        response::partition_response::PartitionResponse,
    },
    replica::ReplicaMetadata,
    segment::metadata::{RecordLocation, SegmentView},
};

#[derive(Clone)]
pub struct PartitionState {
    // TODO: should this have partition_id as well?
    pub segments: Arc<Vec<Arc<SegmentView>>>,
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

    pub fn fetch(&self, partition_index: u32, fetch_req: &FetchPartition) -> PartitionResponse {
        if fetch_req.fetch_offset > self.high_watermark {
            // handle impossible
        }
        let idx = self
            .segments
            .partition_point(|seg| seg.base_offset <= fetch_req.fetch_offset);

        let records = self.segments[idx - 1].clone().fetch(fetch_req);

        PartitionResponse {
            partition_index,
            error_code: 0, // TODO: refer to error code
            high_watermark: self.high_watermark,
            log_start_offset: self.log_end_offset,
            records,
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
        #[allow(deprecated)]
        self.segments[idx - 1].find_pos(offset)
    }

    pub fn ack_replica2(mut self, replica_id: u32, leo: u64) -> Self {
        let replicas = Arc::make_mut(&mut self.replicas);

        if let Some(ref mut replica) = replicas.iter_mut().find(|r| r.replica_id == replica_id) {
            // FIXME: what if an ack for a more recent offset arrived?
            replica.log_end_offset = max(replica.log_end_offset, leo);
        }

        self.high_watermark = replicas.iter().map(|r| r.log_end_offset).min().unwrap();

        self
    }

    #[deprecated(note = "invalid flow, use ack_replica2 instead")]
    pub fn ack_replica(mut self, broker_id: String, offset: u64) -> Self {
        let replicas = Arc::make_mut(&mut self.replicas);

        if let Some(ref mut replica) = replicas.iter_mut().find(|r| r.broker_id == broker_id) {
            // FIXME: what if an ack for a more recent offset arrived?
            replica.log_end_offset = max(replica.log_end_offset, offset);
        }

        self.high_watermark = replicas.iter().map(|r| r.log_end_offset).min().unwrap();

        self
    }

    pub fn consume(
        self,
        segments: Arc<Vec<Arc<SegmentView>>>,
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
