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

    pub fn fetch(&self, partition_index: u32, fetch_req: FetchPartition) -> PartitionResponse {
        if fetch_req.fetch_offset > self.high_watermark {
            // handle impossible
        }
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

    pub fn ack_replica(mut self, replica_id: u32, leo: u64) -> Self {
        let replicas = Arc::make_mut(&mut self.replicas);

        if let Some(ref mut replica) = replicas.iter_mut().find(|r| r.replica_id == replica_id) {
            // FIXME: what if an ack for a more recent offset arrived?
            replica.log_end_offset = max(replica.log_end_offset, leo);
        }

        self.high_watermark = replicas.iter().map(|r| r.log_end_offset).min().unwrap();

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
        Self {
            segments,
            log_end_offset,
            high_watermark,
            ..self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::protocol::fetch::request::fetch_partition::FetchPartition;
    use crate::segment::config::SegmentConfigBuilder;
    use crate::segment::log_segment::LogSegment;
    use crate::storage::record::Record;
    use crate::storage::record_batch::RecordBatch;

    fn make_seg(dir: &tempdir::TempDir, base_offset: u64) -> LogSegment {
        let cfg = SegmentConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .base_offset(base_offset)
            .segment_bytes(1 << 20)
            .index_interval_bytes(1)
            .build()
            .unwrap();
        LogSegment::new(cfg).unwrap()
    }

    fn single_record_batch(base_offset: u64, key: &[u8], val: &[u8]) -> RecordBatch {
        let encoded = Record::new(0, key, val).encode();
        RecordBatch {
            base_offset,
            batch_length: 4 + encoded.len() as u32,
            records_count: 1,
            records: Bytes::from(encoded),
        }
    }

    fn fetch_req(offset: u64) -> FetchPartition {
        FetchPartition {
            partition: 0,
            fetch_offset: offset,
            log_start_offset: None,
            partition_max_bytes: 1 << 20,
            high_watermark: None,
        }
    }

    // Build a state with N segments containing one batch each at their base_offset.
    // Each segment's key is unique: "seg-<base_offset>".
    fn build_state(dirs: &[(&tempdir::TempDir, u64)]) -> PartitionState {
        let mut segs = Vec::new();
        for (dir, base_offset) in dirs {
            let mut seg = make_seg(dir, *base_offset);
            let key = format!("seg-{}", base_offset);
            let batch = single_record_batch(*base_offset, key.as_bytes(), b"val");
            seg.append_batch(&batch).unwrap();
            segs.push(seg.publish());
        }
        let hw = dirs.last().map(|(_, o)| o + 1).unwrap_or(0);
        PartitionState::with_segments(segs, hw)
    }

    fn first_key(resp: &crate::protocol::fetch::response::partition_response::PartitionResponse) -> Vec<u8> {
        let (rec, _) = Record::decode_raw(&resp.records[0].records).unwrap();
        rec.key
    }

    // --- single segment ---

    #[test]
    fn fetch_single_segment_at_offset_zero() {
        let dir = tempdir::TempDir::new("state-test").unwrap();
        let state = build_state(&[(&dir, 0)]);
        let resp = state.fetch(0, fetch_req(0));
        assert!(!resp.records.is_empty(), "must return records");
        assert_eq!(first_key(&resp), b"seg-0");
    }

    // --- two segments: binary search picks the right one ---

    #[test]
    fn fetch_two_segments_selects_first_segment() {
        let dir0 = tempdir::TempDir::new("state-test-0").unwrap();
        let dir1 = tempdir::TempDir::new("state-test-1").unwrap();
        let state = build_state(&[(&dir0, 0), (&dir1, 10)]);

        // offset 0 is in segment 0
        let resp = state.fetch(0, fetch_req(0));
        assert!(!resp.records.is_empty());
        assert_eq!(first_key(&resp), b"seg-0");
    }

    #[test]
    fn fetch_two_segments_selects_second_segment() {
        let dir0 = tempdir::TempDir::new("state-test-0").unwrap();
        let dir1 = tempdir::TempDir::new("state-test-1").unwrap();
        let state = build_state(&[(&dir0, 0), (&dir1, 10)]);

        // offset 10 is exactly the base_offset of segment 1
        let resp = state.fetch(0, fetch_req(10));
        assert!(!resp.records.is_empty());
        assert_eq!(first_key(&resp), b"seg-10");
    }

    #[test]
    fn fetch_offset_inside_first_segment_range() {
        let dir0 = tempdir::TempDir::new("state-test-0").unwrap();
        let dir1 = tempdir::TempDir::new("state-test-1").unwrap();

        // seg0: multi-record batch covering offsets 0..9 — offset 5 lives here
        let mut seg0 = make_seg(&dir0, 0);
        let mut records_payload = Vec::new();
        for i in 0u64..10 {
            records_payload.extend(Record::new(i, format!("k{}", i).as_bytes(), b"v").encode());
        }
        let batch0 = RecordBatch {
            base_offset: 0,
            batch_length: 4 + records_payload.len() as u32,
            records_count: 10,
            records: Bytes::from(records_payload),
        };
        seg0.append_batch(&batch0).unwrap();
        let view0 = seg0.publish();

        // seg1: single record at offset 10
        let mut seg1 = make_seg(&dir1, 10);
        seg1.append_batch(&single_record_batch(10, b"seg-10", b"val")).unwrap();
        let view1 = seg1.publish();

        let state = PartitionState::with_segments(vec![view0, view1], 11);

        // offset 5 → partition_point returns idx=1 (seg1.base_offset=10 > 5), idx-1=0 → seg0
        let resp = state.fetch(0, fetch_req(5));
        assert!(!resp.records.is_empty());
    }

    // --- three segments: binary search on middle ---

    #[test]
    fn fetch_three_segments_selects_first() {
        let dirs: Vec<_> = (0..3).map(|_| tempdir::TempDir::new("st").unwrap()).collect();
        let state = build_state(&[(&dirs[0], 0), (&dirs[1], 100), (&dirs[2], 200)]);

        let resp = state.fetch(0, fetch_req(0));
        assert_eq!(first_key(&resp), b"seg-0");
    }

    #[test]
    fn fetch_three_segments_selects_middle() {
        let dirs: Vec<_> = (0..3).map(|_| tempdir::TempDir::new("st").unwrap()).collect();
        let state = build_state(&[(&dirs[0], 0), (&dirs[1], 100), (&dirs[2], 200)]);

        let resp = state.fetch(0, fetch_req(100));
        assert_eq!(first_key(&resp), b"seg-100");
    }

    #[test]
    fn fetch_three_segments_selects_last() {
        let dirs: Vec<_> = (0..3).map(|_| tempdir::TempDir::new("st").unwrap()).collect();
        let state = build_state(&[(&dirs[0], 0), (&dirs[1], 100), (&dirs[2], 200)]);

        let resp = state.fetch(0, fetch_req(200));
        assert_eq!(first_key(&resp), b"seg-200");
    }

    #[test]
    fn fetch_offset_between_second_and_third_selects_second() {
        let dirs: Vec<_> = (0..3).map(|_| tempdir::TempDir::new("st").unwrap()).collect();

        // seg0: offset 0
        let mut seg0 = make_seg(&dirs[0], 0);
        seg0.append_batch(&single_record_batch(0, b"seg-0", b"val")).unwrap();
        let view0 = seg0.publish();

        // seg1: multi-record batch covering offsets 100..199 — offset 150 lives here
        let mut seg1 = make_seg(&dirs[1], 100);
        let mut payload = Vec::new();
        for i in 0u64..100 {
            payload.extend(Record::new(i, format!("k{}", i).as_bytes(), b"v").encode());
        }
        let batch1 = RecordBatch {
            base_offset: 100,
            batch_length: 4 + payload.len() as u32,
            records_count: 100,
            records: Bytes::from(payload),
        };
        seg1.append_batch(&batch1).unwrap();
        let view1 = seg1.publish();

        // seg2: offset 200
        let mut seg2 = make_seg(&dirs[2], 200);
        seg2.append_batch(&single_record_batch(200, b"seg-200", b"val")).unwrap();
        let view2 = seg2.publish();

        let state = PartitionState::with_segments(vec![view0, view1, view2], 201);

        // offset 150 → partition_point returns idx=2 (seg2.base_offset=200 > 150), idx-1=1 → seg1
        let resp = state.fetch(0, fetch_req(150));
        assert!(!resp.records.is_empty());
    }

    // --- response metadata ---

    #[test]
    fn fetch_response_carries_high_watermark() {
        let dir = tempdir::TempDir::new("state-test").unwrap();
        let state = build_state(&[(&dir, 0)]);
        let resp = state.fetch(0, fetch_req(0));
        assert_eq!(resp.high_watermark, state.high_watermark);
    }

    #[test]
    fn fetch_response_carries_partition_index() {
        let dir = tempdir::TempDir::new("state-test").unwrap();
        let state = build_state(&[(&dir, 0)]);
        let resp = state.fetch(42, fetch_req(0));
        assert_eq!(resp.partition_index, 42);
    }

    #[test]
    fn fetch_response_error_code_is_none() {
        use crate::protocol::error_codes::ErrorCode;
        let dir = tempdir::TempDir::new("state-test").unwrap();
        let state = build_state(&[(&dir, 0)]);
        let resp = state.fetch(0, fetch_req(0));
        assert_eq!(resp.error_code, ErrorCode::None);
    }
}
