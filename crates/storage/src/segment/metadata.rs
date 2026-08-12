use std::{fs::File, os::unix::fs::FileExt, sync::Arc};

use memmap::{Mmap, MmapOptions};

use proto::{
    batch_attributes::BatchAttributes, batch_iter::BatchIter, error::ProtoError,
    fetch::request::fetch_partition::FetchPartition, record::Record, record_batch::RecordBatch,
};
use tokio::io::AsyncSeekExt;

const INDEX_ENTRY_SIZE: usize = 16;

pub type RecordLocation = u64;

pub struct IndexEntry {
    offset: u64,
    pos: u64,
}

#[derive(Clone)]
pub struct SegmentView {
    pub base_offset: u64,

    log: Arc<File>,
    index: Arc<Mmap>,

    pub index_count: usize,
    pub size: usize,
}

impl SegmentView {
    pub fn new(base_offset: u64, log: File, index: File) -> Self {
        let index = unsafe { MmapOptions::new().map(&index).unwrap() };

        Self {
            base_offset,
            log: Arc::new(log),
            index: Arc::new(index),
            index_count: 0,
            size: 0,
        }
    }

    pub fn batch_iter_from(&self, offset: u64) -> Result<BatchIter, ProtoError> {
        let start = match self.find_physical_position(offset) {
            Some(idx) => idx,
            None if self.size > 0 => IndexEntry {
                offset: self.base_offset,
                pos: 0,
            },
            None => {
                return Ok(BatchIter::empty(self.log.clone()));
            }
        };

        let pos = self.linear_search(offset, start).map_err(ProtoError::Io)?;

        Ok(BatchIter {
            file: self.log.clone(),
            pos,
        })
    }

    pub fn batches_from(&self, pos: u64) -> BatchIter {
        BatchIter {
            file: self.log.clone(),
            pos,
        }
    }

    pub fn fetch_all(&self) -> Vec<RecordBatch> {
        // TODO: handle error
        self.batches_from(0).map(|b| b.unwrap()).collect()
    }

    pub fn fetch(&self, req: FetchPartition) -> Vec<RecordBatch> {
        let start = match self.find_physical_position(req.fetch_offset) {
            Some(idx) => idx,
            None if self.size > 0 => IndexEntry {
                offset: self.base_offset,
                pos: 0,
            },
            None => return vec![],
        };

        let Ok(mut pos) = self.linear_search(req.fetch_offset, start) else {
            return vec![];
        };

        if pos >= self.size as u64 {
            return vec![];
        }

        let batch_iter = self.batches_from(pos);
        let batch = batch_iter.next().unwrap().unwrap();

        let target_delta = req.fetch_offset.saturating_sub(batch.base_offset);
        let record_iter = batch.records_iter();
        let mut skipped_count = 0u32;
        let starting_pos = record_iter
            .find_map(|(pos, record)| {
                if record.offset_delta == target_delta {
                    Some(pos)
                } else {
                    skipped_count += 1;
                    None
                }
            })
            .unwrap_or(0);

        let sliced_records = batch.records.slice(starting_pos..);
        let actual_batch_length = sliced_records.len() as u32 + 4;

        let mut batches = vec![RecordBatch {
            base_offset: req.fetch_offset,
            batch_length: actual_batch_length,
            records_count: batch.records_count.saturating_sub(skipped_count),
            records: sliced_records,
            partition_leader_epoch: -1,
            magic: 2,
            crc: todo!(),
            attributes: BatchAttributes(0),
            last_offset_delta: todo!(),
            base_timestamp: todo!(),
            max_timestamp: todo!(),
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
        }];

        let mut total_bytes = actual_batch_length;
        pos += 12 + batch.batch_length as u64;
        while total_bytes < req.partition_max_bytes {
            if pos + 12 > self.size as u64 {
                break;
            }
            let batch = RecordBatch::decode_file(&self.log, pos);
            if 12 + batch.batch_length as u64 > self.size as u64 - pos {
                break;
            }
            total_bytes += batch.batch_length;
            pos += 12 + batch.batch_length as u64;
            if total_bytes <= req.partition_max_bytes {
                batches.push(batch);
            }
        }

        batches
    }

    pub fn with_metadata(&self, index_count: usize, size: usize) -> Arc<Self> {
        Arc::new(Self {
            base_offset: self.base_offset,
            log: self.log.clone(),
            index: self.index.clone(),
            size,
            index_count,
        })
    }

    fn find_physical_position(&self, target_offset: u64) -> Option<IndexEntry> {
        if self.index_count == 0 {
            return None;
        }

        let mut lo = 0usize;
        let mut hi = self.index_count - 1;

        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let base = mid * INDEX_ENTRY_SIZE;

            let offset = u64::from_be_bytes(self.index[base..base + 8].try_into().unwrap());

            if offset <= target_offset {
                lo = mid + 1;
            } else {
                if mid == 0 {
                    break;
                }
                hi = mid - 1;
            }
        }

        if lo == 0 {
            return None;
        }

        let entry = lo - 1;
        let base = entry * INDEX_ENTRY_SIZE;

        let offset = u64::from_be_bytes(self.index[base..base + 8].try_into().unwrap());
        let pos = u64::from_be_bytes(self.index[base + 8..base + 16].try_into().unwrap());

        Some(IndexEntry { offset, pos })
    }

    pub fn linear_search(&self, target_offset: u64, idx: IndexEntry) -> std::io::Result<u64> {
        let mut offset = idx.offset;
        let mut pos = idx.pos;

        while offset < target_offset {
            if pos >= self.size as u64 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "offset beyond end of segment",
                ));
            }
            let _ = read_u64_at(&self.log, pos)?;
            let batch_length = read_u32_at(&self.log, pos + 8)?;
            let records_count = read_u32_at(&self.log, pos + 12)?;

            if offset + records_count as u64 > target_offset {
                break;
            }

            pos += 12 + batch_length as u64;
            offset += records_count as u64;
        }

        Ok(pos)
    }
}

fn read_u32_at(file: &File, pos: u64) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    file.read_at(&mut buf, pos)?;
    Ok(u32::from_be_bytes(buf))
}

fn read_u64_at(file: &File, pos: u64) -> std::io::Result<u64> {
    let mut buf = [0u8; 8];
    file.read_at(&mut buf, pos)?;
    Ok(u64::from_be_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::config::SegmentConfigBuilder;
    use crate::segment::log_segment::LogSegment;
    use bytes::Bytes;
    use proto::record::Record;
    use proto::record_batch::RecordBatch;

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

    fn single_record_batch(
        base_offset: u64,
        offset_delta: u64,
        key: &[u8],
        val: &[u8],
    ) -> RecordBatch {
        let record = Record::new(offset_delta, key, val);
        let encoded = record.encode();
        RecordBatch {
            base_offset,
            batch_length: 4 + encoded.len() as u32,
            records_count: 1,
            records: Bytes::from(encoded),
        }
    }

    fn fetch_req(offset: u64, max_bytes: u32) -> FetchPartition {
        FetchPartition {
            partition: 0,
            fetch_offset: offset,
            partition_max_bytes: max_bytes,
            high_watermark: 0,
        }
    }

    #[test]
    fn find_physical_position_returns_none_when_no_index() {
        let dir = tempdir::TempDir::new("meta-test").unwrap();
        let mut seg = make_seg(&dir, 0);
        let view = seg.publish();
        let batches = view.fetch(fetch_req(0, 1 << 20));
        assert!(batches.is_empty(), "no index => empty fetch");
    }

    #[test]
    fn fetch_returns_batch_matching_fetch_offset() {
        let dir = tempdir::TempDir::new("meta-test").unwrap();
        let mut seg = make_seg(&dir, 0);

        let batch = single_record_batch(0, 0, b"key", b"value");
        seg.append_batch(&batch).unwrap();
        let view = seg.publish();

        let batches = view.fetch(fetch_req(0, 1 << 20));
        assert!(!batches.is_empty());
        assert_eq!(batches[0].base_offset, 0);
    }
}
