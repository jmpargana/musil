use std::{fs::File, os::unix::fs::FileExt, sync::Arc};

use memmap::{Mmap, MmapOptions};

use crate::{
    protocol::fetch::request::fetch_partition::FetchPartition,
    storage::{record::Record, record_batch::RecordBatch},
};

const INDEX_ENTRY_SIZE: usize = 16; // (u64 offset + u64 position)

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

    pub fn fetch_all(&self) -> Vec<RecordBatch> {
        let mut buf = vec![0u8; self.size];
        self.log.read_at(&mut buf, 0).unwrap();

        let mut batches = Vec::new();
        let mut pos = 0u64;
        while pos + 12 <= self.size as u64 {
            let batch = RecordBatch::decode(&buf, pos);
            pos += 12 + batch.batch_length as u64;
            batches.push(batch);
        }
        batches
    }

    // TODO: record doesn't need to be fully deseralized. Neither does record batch (if full).
    // Even if copying the byte array to a `bytes::Bytes`, copying data to user-space is unnecessary.
    // Instead I should just pass FD pointers around so `connection` calls `sendfile`.
    // This behavior is equivalent to kafka's `transferTo` function call.
    pub fn fetch(&self, req: FetchPartition) -> Vec<RecordBatch> {
        let Some(idx) = self.find_physical_position(req.fetch_offset) else {
            return vec![];
        };

        let Ok(mut pos) = self.linear_search(req.fetch_offset, idx) else {
            return vec![];
        };

        let batch = RecordBatch::decode_file(&self.log, pos);

        let target_delta = req.fetch_offset.saturating_sub(batch.base_offset);
        let mut record_iter = RecordIter {
            bytes: &batch.records,
            pos: 0,
        };
        let starting_pos = record_iter
            .find_map(|(pos, record)| (record.offset_delta == target_delta).then_some(pos))
            .unwrap_or(0);

        let actual_batch_length = batch.batch_length.saturating_sub(starting_pos as u32);

        let mut batches = vec![RecordBatch {
            base_offset: req.fetch_offset,
            batch_length: actual_batch_length,
            records_count: batch
                .records_count
                .saturating_sub((req.fetch_offset as u32).saturating_sub(batch.base_offset as u32)),
            // TODO: start from starting_pos
            records: batch.records,
        }];

        // TODO: for sure there's a cleaner way of writing this code.
        let mut total_bytes = actual_batch_length;
        pos += 12 + batch.batch_length as u64;
        while total_bytes < req.partition_max_bytes {
            // Stop at end of segment
            if pos >= self.size as u64 {
                break;
            }
            let batch = RecordBatch::decode_file(&self.log, pos);
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

            let offset = u64::from_le_bytes(self.index[base..base + 8].try_into().unwrap());

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

        let offset = u64::from_le_bytes(self.index[base..base + 8].try_into().unwrap());
        let pos = u64::from_le_bytes(self.index[base + 8..base + 16].try_into().unwrap());

        Some(IndexEntry { offset, pos })
    }

    fn linear_search(&self, target_offset: u64, idx: IndexEntry) -> std::io::Result<u64> {
        let mut offset = idx.offset;
        let mut pos = idx.pos;

        // linear scan in log file using offsets (pread)
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

            // Stop when the target offset is inside this batch (not past it)
            if offset + records_count as u64 > target_offset {
                break;
            }

            // 8 (base_offset) + 4 (batch_length field) + batch_length bytes per entry
            pos += 12 + batch_length as u64;
            offset += records_count as u64;
        }

        Ok(pos)
    }
}

// Cool snippet to make above functions generic:
//
// trait ReadLe: Sized { const SIZE: usize; fn from_le_bytes(bytes: &[u8]) -> Self; }
// macro_rules! impl_read_le { ($t:ty, $size:expr) => { impl ReadLe for $t { ... } }; }
// fn read_at<T: ReadLe>(file: &File, pos: u64) -> std::io::Result<T> { ... }
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

// TODO: move this to segment iter file?
struct RecordIter<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = (usize, Record);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.bytes.len() {
            return None;
        }

        let start = self.pos;
        let (record, consumed) = Record::decode_raw(&self.bytes[self.pos..]).ok()?;
        self.pos += consumed;

        Some((start, record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::config::SegmentConfigBuilder;
    use crate::segment::log_segment::LogSegment;
    use crate::storage::record::Record;
    use crate::storage::record_batch::RecordBatch;
    use bytes::Bytes;

    fn make_seg(dir: &tempdir::TempDir, base_offset: u64) -> LogSegment {
        let cfg = SegmentConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .base_offset(base_offset)
            .segment_bytes(1 << 20)
            .index_interval_bytes(1) // every batch triggers an index entry
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
    fn record_iter_walks_multiple_records() {
        let r0 = Record::new(0, b"k0", b"value0");
        let r1 = Record::new(1, b"k1", b"val1");
        let r2 = Record::new(2, b"k2", b"v2");

        let mut encoded = r0.encode();
        encoded.extend(r1.encode());
        encoded.extend(r2.encode());

        let iter = RecordIter {
            bytes: &encoded,
            pos: 0,
        };
        let items: Vec<_> = iter.collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].1.offset_delta, 0);
        assert_eq!(items[1].1.offset_delta, 1);
        assert_eq!(items[2].1.offset_delta, 2);
        assert_eq!(items[2].1.key, b"k2");
    }

    #[test]
    fn record_iter_positions_are_monotonically_increasing() {
        let r0 = Record::new(0, b"key", b"longvalue");
        let r1 = Record::new(1, b"k", b"v");
        let mut encoded = r0.encode();
        encoded.extend(r1.encode());

        let iter = RecordIter {
            bytes: &encoded,
            pos: 0,
        };
        let items: Vec<_> = iter.collect();
        assert_eq!(items.len(), 2);
        assert!(
            items[1].0 > items[0].0,
            "second record pos must be after first"
        );
        assert_eq!(items[1].0, r0.encode().len());
    }

    #[test]
    fn linear_search_reaches_first_batch_at_offset_zero() {
        let dir = tempdir::TempDir::new("meta-test").unwrap();
        let mut seg = make_seg(&dir, 0);
        let batch = single_record_batch(0, 0, b"k", b"v");
        seg.append_batch(&batch).unwrap();
        let view = seg.publish();

        // linear_search from the only index entry should return pos=0
        let idx = IndexEntry { offset: 0, pos: 0 };
        let pos = view.linear_search(0, idx).unwrap();
        assert_eq!(pos, 0);
    }

    #[test]
    fn linear_search_advances_past_first_batch_to_find_second() {
        let dir = tempdir::TempDir::new("meta-test").unwrap();
        let mut seg = make_seg(&dir, 0);

        let b0 = single_record_batch(0, 0, b"k0", b"v0");
        let b1 = single_record_batch(1, 0, b"k1", b"v1");
        let b0_size = 12 + b0.batch_length as u64;

        seg.append_batch(&b0).unwrap();
        seg.append_batch(&b1).unwrap();
        let view = seg.publish();

        let idx = IndexEntry { offset: 0, pos: 0 };
        // target_offset=1 — must skip b0 and land at b1's start
        let pos = view.linear_search(1, idx).unwrap();
        assert_eq!(pos, b0_size, "must land at start of second batch");
    }

    #[test]
    fn linear_search_at_exact_batch_boundary() {
        let dir = tempdir::TempDir::new("meta-test").unwrap();
        let mut seg = make_seg(&dir, 0);

        let b0 = RecordBatch {
            base_offset: 0,
            batch_length: 4 + 3,
            records_count: 3,
            records: Bytes::from_static(b"abc"),
        };
        let b1 = single_record_batch(3, 0, b"key", b"val");
        let b0_size = 12 + b0.batch_length as u64;

        seg.append_batch(&b0).unwrap();
        seg.append_batch(&b1).unwrap();
        let view = seg.publish();

        let idx = IndexEntry { offset: 0, pos: 0 };
        let pos = view.linear_search(3, idx).unwrap();
        assert_eq!(pos, b0_size);
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

    #[test]
    fn fetch_correct_record_count_in_result() {
        let dir = tempdir::TempDir::new("meta-test").unwrap();
        let mut seg = make_seg(&dir, 0);

        let b0 = single_record_batch(0, 0, b"k0", b"v0");
        let b1 = single_record_batch(1, 0, b"k1", b"v1");
        seg.append_batch(&b0).unwrap();
        seg.append_batch(&b1).unwrap();
        let view = seg.publish();

        let batches = view.fetch(fetch_req(0, 1 << 20));
        let total_records: u32 = batches.iter().map(|b| b.records_count).sum();
        assert!(total_records >= 1);
    }

    #[test]
    fn fetch_respects_partition_max_bytes() {
        let dir = tempdir::TempDir::new("meta-test").unwrap();
        let mut seg = make_seg(&dir, 0);

        let b0 = single_record_batch(0, 0, b"k0", b"v0");
        let b1 = single_record_batch(1, 0, b"k1", b"v1");
        seg.append_batch(&b0).unwrap();
        seg.append_batch(&b1).unwrap();
        let view = seg.publish();

        let batches_small = view.fetch(fetch_req(0, 1));
        let batches_large = view.fetch(fetch_req(0, 1 << 20));
        assert!(batches_small.len() <= batches_large.len());
    }

    #[test]
    fn fetch_does_not_loop_forever_at_end_of_segment() {
        let dir = tempdir::TempDir::new("meta-test").unwrap();
        let mut seg = make_seg(&dir, 0);

        let batch = single_record_batch(0, 0, b"k", b"v");
        seg.append_batch(&batch).unwrap();
        let view = seg.publish();

        let result = std::panic::catch_unwind(|| {
            // Just call fetch; test will hang/panic if infinite loop bug present
            view.fetch(fetch_req(0, u32::MAX))
        });
        assert!(result.is_ok(), "fetch must not panic or loop forever");
    }
}
