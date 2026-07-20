use std::{cmp::max, sync::Arc};

use crate::{
    protocol::{
        error_codes::ErrorCode,
        fetch::{
            request::fetch_partition::FetchPartition,
            response::partition_response::PartitionResponse,
        },
    },
    replica::ReplicaMetadata,
    segment::metadata::SegmentView,
    storage::record_batch::RecordBatch,
};

#[derive(Clone)]
pub struct PartitionState {
    pub segments: Arc<Vec<Arc<SegmentView>>>,
    pub log_end_offset: u64,
    pub high_watermark: u64,
    pub replicas: Arc<Vec<ReplicaMetadata>>,
}

impl PartitionState {
    pub fn new(replicas: Vec<ReplicaMetadata>) -> Self {
        Self {
            segments: Arc::new(vec![]),
            log_end_offset: 0,
            high_watermark: 0,
            replicas: Arc::new(replicas),
        }
    }

    pub fn fetch(&self, partition_index: u32, fetch_req: FetchPartition) -> PartitionResponse {
        let idx = self
            .segments
            .partition_point(|seg| seg.base_offset <= fetch_req.fetch_offset);

        let records = self.segments[idx - 1].clone().fetch(fetch_req);

        PartitionResponse {
            partition_index,
            error_code: ErrorCode::None,
            high_watermark: self.high_watermark,
            log_start_offset: self.log_end_offset,
            records,
        }
    }

    pub fn fetch_all(&self) -> PartitionResponse {
        let records = self
            .segments
            .iter()
            .flat_map(|seg| seg.fetch_all())
            .collect::<Vec<RecordBatch>>();

        PartitionResponse {
            partition_index: 0,
            error_code: ErrorCode::None,
            high_watermark: 0,
            log_start_offset: 0,
            records,
        }
    }

    pub fn ack_replica(mut self, replica_id: u32, leo: u64) -> Self {
        let replicas = Arc::make_mut(&mut self.replicas);

        if let Some(ref mut replica) = replicas.iter_mut().find(|r| r.replica_id == replica_id) {
            replica.log_end_offset = max(replica.log_end_offset, leo);
        }

        if let Some(min_leo) = replicas.iter().map(|r| r.log_end_offset).min() {
            self.high_watermark = min_leo;
        }

        self
    }

    #[cfg(test)]
    pub fn with_segments(segments: Vec<Arc<SegmentView>>, high_watermark: u64) -> Self {
        Self {
            segments: Arc::new(segments),
            log_end_offset: high_watermark,
            high_watermark,
            replicas: Arc::new(vec![]),
        }
    }

    pub fn consume(
        self,
        segments: Arc<Vec<Arc<SegmentView>>>,
        log_end_offset: u64,
        high_watermark: u64,
    ) -> Self {
        Self { segments, log_end_offset, high_watermark, ..self }
    }
}
