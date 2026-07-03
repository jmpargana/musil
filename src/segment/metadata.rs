use std::{fs::File, os::unix::fs::FileExt, sync::atomic::AtomicU64};

use memmap::{Mmap, MmapOptions};

const INDEX_ENTRY_SIZE: usize = 16; // (u64 offset + u64 position)

pub type RecordLocation = u64;

pub struct Segment {
    pub base_offset: u64,
    log: File,
    index: Mmap,

    // TODO: tricky, not sure how to solve this. We only need this at the end
    index_count: usize,
    pub size: usize, // TODO: why is it AtomicU64?
}

impl Segment {
    pub fn new(base_offset: u64, log: File, index: File) -> Self {
        let index = unsafe { MmapOptions::new().map(&index).unwrap() };

        Self {
            base_offset,
            log,
            index,
            index_count: 0,
            size: 0,
        }
    }

    pub fn find_pos(&self, target_offset: u64) -> Option<RecordLocation> {
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

        let mut offset = u64::from_le_bytes(self.index[base..base + 8].try_into().unwrap());
        let mut pos = u64::from_le_bytes(self.index[base + 8..base + 16].try_into().unwrap());

        // linear scan in log file using offsets (pread)
        while offset < target_offset {
            let mut len_buf = [0u8; 4];
            self.log
                .read_at(&mut len_buf, pos)
                .expect("physical byte position should exist");

            let size = u32::from_le_bytes(len_buf) as u64;

            pos += 4 + size;
            offset += 1;
        }

        Some(pos)
    }
}
