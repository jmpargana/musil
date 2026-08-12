use bytes::{Buf, Bytes};

use crate::{error::ProtoError, record::Record};

pub struct RecordIter {
    pub buf: Bytes,
}

impl Iterator for RecordIter {
    type Item = Result<Record, ProtoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.buf.has_remaining() {
            return None;
        }

        Some(Record::decode(&mut self.buf).map_err(ProtoError::Io))
    }
}
