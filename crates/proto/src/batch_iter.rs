use std::{fs::File, os::unix::fs::FileExt, sync::Arc};

use bytes::{Buf, BytesMut};
use tokio::io::BufStream;
use tokio_stream::Stream;

use crate::{error::ProtoError, record_batch::RecordBatch};

pub struct BatchIter {
    pub file: Arc<File>,
    pub pos: u64,
}

impl Iterator for BatchIter {
    type Item = Result<RecordBatch, ProtoError>;

    // FIXME: read_at may return Ok(n) where n < header
    fn next(&mut self) -> Option<Self::Item> {
        let mut header = BytesMut::zeroed(12);
        if let Err(e) = self.file.read_at(&mut header, self.pos) {
            return Some(Err(ProtoError::Io(e)));
        }

        let mut peek = header.clone();

        let _base_offset = peek.get_u64();
        let batch_length = peek.get_u32();

        let mut batch = BytesMut::zeroed(12 + batch_length as usize);

        batch[..12].copy_from_slice(&header);

        if let Err(e) = self.file.read_at(&mut batch[12..], self.pos + 12) {
            return Some(Err(ProtoError::Io(e)));
        }

        self.pos += 12 + batch_length as u64;

        Some(RecordBatch::decode(batch.freeze()))
    }
}
