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

    // TODO: record doesn't need to be fully deseralized. Neither does record batch (if full).
    // Even if copying the byte array to a `bytes::Bytes`, copying data to user-space is unnecessary.
    // Instead I should just pass FD pointers around so `connection` calls `sendfile`.
    // This behavior is equivalent to kafka's `transferTo` function call.
    pub fn fetch(&self, req: &FetchPartition) -> Vec<RecordBatch> {
        let Some(idx) = self.find_physical_position(req.fetch_offset) else {
            return vec![];
        };

        let Ok(mut pos) = self.linear_search(req.fetch_offset, idx) else {
            return vec![];
        };

        let batch = RecordBatch::decode_file(&self.log, pos);

        let mut record_iter = RecordIter {
            bytes: &batch.records,
            pos: 0,
        };
        let starting_pos = record_iter
            .find_map(|(pos, record)| {
                (record.offset_delta.unwrap() == req.fetch_offset).then_some(pos)
            })
            .unwrap();

        let actual_batch_length = batch.batch_length - starting_pos as u32;

        let mut batches = vec![RecordBatch {
            base_offset: req.fetch_offset,
            batch_length: actual_batch_length,
            records_count: batch.records_count
                - (req.fetch_offset as u32 - batch.base_offset as u32),
            // TODO: start from starting_pos
            records: batch.records,
        }];

        // TODO: for sure there's a cleaner way of writing this code.
        let mut total_bytes = actual_batch_length;
        pos += 8 + batch.batch_length as u64;
        while total_bytes < req.partition_max_bytes {
            let batch = RecordBatch::decode_file(&self.log, pos);
            total_bytes += batch.batch_length;
            if total_bytes < req.partition_max_bytes {
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
            // TODO: do I need base offset here?
            let _ = read_u64_at(&self.log, pos)?;
            let batch_length = read_u32_at(&self.log, pos + 8)?;
            let records_count = read_u32_at(&self.log, pos + 8 + 4)?;

            if offset + records_count as u64 >= target_offset {
                break;
            }

            // FIXME: need to define jump size
            pos += 8 + batch_length as u64;
            offset += records_count as u64;
        }

        Ok(pos)
    }
}

/**
 * Cool snippet to make above functions generic:
 *
 * use std::fs::File;
use std::os::unix::fs::FileExt;

trait ReadLe: Sized {
    const SIZE: usize;
    fn from_le_bytes(bytes: &[u8]) -> Self;
}

macro_rules! impl_read_le {
    ($t:ty, $size:expr) => {
        impl ReadLe for $t {
            const SIZE: usize = $size;

            fn from_le_bytes(bytes: &[u8]) -> Self {
                <$t>::from_le_bytes(bytes.try_into().unwrap())
            }
        }
    };
}

impl_read_le!(u16, 2);
impl_read_le!(u32, 4);
impl_read_le!(u64, 8);
impl_read_le!(u128, 16);
impl_read_le!(i16, 2);
impl_read_le!(i32, 4);
impl_read_le!(i64, 8);
impl_read_le!(i128, 16);

fn read_at<T: ReadLe>(file: &File, pos: u64) -> std::io::Result<T> {
    let mut buf = vec![0u8; T::SIZE];
    file.read_at(&mut buf, pos)?;
    Ok(T::from_le_bytes(&buf))
}
 */
fn read_u32_at(file: &File, pos: u64) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    file.read_at(&mut buf, pos)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64_at(file: &File, pos: u64) -> std::io::Result<u64> {
    let mut buf = [0u8; 8];
    file.read_at(&mut buf, pos)?;
    Ok(u64::from_le_bytes(buf))
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

    // TODO: implement these test cases.
    #[test]
    fn appends_total_amount_as_expected() {}
    #[test]
    fn finds_binary_search_correctly() {}
    #[test]
    fn finds_linear_search_correctly() {}
    #[test]
    fn finds_spot_inside_batch() {}
    #[test]
    fn returns_partial_batch() {}
    #[test]
    fn returns_batch_if_below_space() {}
    #[test]
    fn includes_more_batches_if_max_bytes() {}
}
