use std::sync::Arc;

use bytes::Bytes;
use proto::record::Record;
use proto::record_batch::RecordBatch;
use raft::{LogEntry, RaftLog};

use crate::partition::handle::PartitionHandle;

pub struct RaftPartition {
    handle: Arc<PartitionHandle>,
}

impl RaftPartition {
    pub fn new(handle: Arc<PartitionHandle>) -> Self {
        Self { handle }
    }
}

impl Clone for RaftPartition {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
        }
    }
}

impl RaftLog for RaftPartition {
    fn log_end_offset(&self) -> u64 {
        self.handle.state.load_full().log_end_offset
    }

    fn epoch_at(&self, offset: u64) -> Option<u32> {
        let state = self.handle.state.load_full();
        let idx = state.segments.partition_point(|seg| seg.base_offset <= offset);
        if idx == 0 {
            return None;
        }

        let segment = &state.segments[idx - 1];
        let iter = match segment.batch_iter_from(offset) {
            Ok(iter) => iter,
            Err(_) => return None,
        };

        for batch in iter {
            let Ok(batch) = batch else { break };
            if batch.base_offset == offset {
                return Some(batch.partition_leader_epoch as u32);
            }
            if batch.base_offset > offset {
                break;
            }
        }

        None
    }

    fn last_epoch(&self) -> u32 {
        let state = self.handle.state.load_full();
        if state.segments.is_empty() {
            return 0;
        }

        let mut prev_epoch: u32 = 0;
        let mut curr_epoch: u32 = 0;

        for segment in state.segments.iter() {
            let iter = segment.batches_from(0);
            for batch in iter {
                let Ok(batch) = batch else { break };
                let epoch = batch.partition_leader_epoch as u32;
                if epoch != curr_epoch {
                    prev_epoch = curr_epoch;
                    curr_epoch = epoch;
                }
            }
        }

        prev_epoch
    }

    fn entries(&self, start: u64, end: u64) -> Vec<LogEntry> {
        let state = self.handle.state.load_full();
        if state.segments.is_empty() {
            return vec![];
        }

        let idx = state.segments.partition_point(|seg| seg.base_offset <= start);
        let start_idx = if idx == 0 { 0 } else { idx - 1 };

        let mut result = Vec::new();

        for segment in &state.segments[start_idx..] {
            let iter = match segment.batch_iter_from(start) {
                Ok(iter) => iter,
                Err(_) => continue,
            };

            for batch in iter {
                let Ok(batch) = batch else { break };
                if batch.base_offset >= end {
                    return result;
                }
                if batch.base_offset < start {
                    continue;
                }

                let data = Record::decode(&mut batch.records.clone())
                    .map(|r| r.value)
                    .unwrap_or_default();

                result.push(LogEntry {
                    epoch: batch.partition_leader_epoch as u32,
                    offset: batch.base_offset,
                    data,
                });
            }
        }

        result
    }

    fn find_epoch_start(&self, epoch: u32) -> u64 {
        let state = self.handle.state.load_full();
        if state.segments.is_empty() {
            return 0;
        }

        for segment in state.segments.iter() {
            let iter = segment.batches_from(0);
            for batch in iter {
                let Ok(batch) = batch else { break };
                if batch.partition_leader_epoch as u32 == epoch {
                    return batch.base_offset;
                }
            }
        }

        state.log_end_offset
    }

    async fn append(&mut self, entry: LogEntry) {
        use proto::produce::acks::Acks;

        let record = Record::new(0, b"", &entry.data);
        let encoded = record.encode();
        let records = Bytes::from(encoded);
        let batch_length = 49 + records.len() as u32;
        let mut batch = RecordBatch::from_compact(entry.offset, batch_length, 1, records);
        batch.partition_leader_epoch = entry.epoch as i32;
        self.handle.append(batch, Acks::None).await;
    }

    async fn truncate(&mut self, offset: u64) {
        self.handle.truncate(offset).await;
    }
}
