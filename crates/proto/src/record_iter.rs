use bytes::{Buf, Bytes};

use crate::{error::ProtoError, record::Record};

pub struct RecordIter {
    buf: Bytes,
    pos: usize,
    total_len: usize,
}

impl RecordIter {
    pub fn new(buf: Bytes) -> Self {
        let total_len = buf.len();
        Self {
            buf,
            pos: 0,
            total_len,
        }
    }

    pub fn position(&self) -> usize {
        self.pos
    }
}

impl Iterator for RecordIter {
    type Item = Result<(usize, Record), ProtoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.buf.has_remaining() {
            return None;
        }

        let start = self.pos;
        match Record::decode(&mut self.buf) {
            Ok(record) => {
                self.pos = self.total_len - self.buf.remaining();
                Some(Ok((start, record)))
            }
            Err(e) => Some(Err(ProtoError::Io(e))),
        }
    }
}
