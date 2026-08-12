use bytes::{Buf, Bytes};

use crate::error::ProtoError;

pub struct RecordRef {
    pub offset_delta: u64,
    pub byte_offset: usize,
    pub byte_len: usize,
    pub index: u32,
}

impl RecordRef {
    pub fn record_index(&self) -> u32 {
        self.index
    }
}

pub struct RecordRefIter {
    buf: Bytes,
    pos: usize,
    total_len: usize,
    index: u32,
}

impl RecordRefIter {
    pub fn new(buf: Bytes) -> Self {
        let total_len = buf.len();
        Self {
            buf,
            pos: 0,
            total_len,
            index: 0,
        }
    }
}

impl Iterator for RecordRefIter {
    type Item = Result<RecordRef, ProtoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.buf.has_remaining() {
            return None;
        }

        let start = self.pos;

        if self.buf.remaining() < 4 {
            return Some(Err(ProtoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "not enough bytes for record length",
            ))));
        }

        let length = self.buf.get_u32() as usize;
        let data_len = length - 4;

        if self.buf.remaining() < data_len {
            return Some(Err(ProtoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "record truncated",
            ))));
        }

        let _attributes = self.buf.get_u8();
        let _timestamp_delta = self.buf.get_u64();
        let offset_delta = self.buf.get_u64();

        let already_consumed = 1 + 8 + 8;
        self.buf.advance(data_len - already_consumed);

        let byte_len = 4 + length;
        self.pos = self.total_len - self.buf.remaining();

        let idx = self.index;
        self.index += 1;

        Some(Ok(RecordRef {
            offset_delta,
            byte_offset: start,
            byte_len,
            index: idx,
        }))
    }
}
