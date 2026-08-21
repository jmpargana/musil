use std::{fs::File, os::unix::fs::FileExt, sync::Arc};

use crate::{
    error::ProtoError,
    record_batch::{BATCH_HEADER_PREFIX, RecordBatch},
};

pub struct BatchIter {
    pub file: Arc<File>,
    pub pos: u64,
    pub end: u64,
}

impl BatchIter {
    pub fn empty(file: Arc<File>) -> Self {
        Self {
            file,
            pos: 0,
            end: 0,
        }
    }
}

impl Iterator for BatchIter {
    type Item = Result<RecordBatch, ProtoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.end {
            return None;
        }

        let mut header = [0u8; BATCH_HEADER_PREFIX];
        if let Err(e) = self.file.read_at(&mut header, self.pos) {
            return Some(Err(ProtoError::Io(e)));
        }

        let _base_offset = u64::from_be_bytes(header[0..8].try_into().unwrap());
        let batch_length = u32::from_be_bytes(header[8..12].try_into().unwrap());

        if self.pos + BATCH_HEADER_PREFIX as u64 + u64::from(batch_length) > self.end {
            return None;
        }

        let batch = RecordBatch::decode_file(&self.file, self.pos);
        self.pos += BATCH_HEADER_PREFIX as u64 + u64::from(batch_length);

        Some(Ok(batch))
    }
}
