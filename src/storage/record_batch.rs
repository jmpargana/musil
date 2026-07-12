use std::{fs::File, os::unix::fs::FileExt};

use bytes::{Buf, BufMut, Bytes, BytesMut};

// TODO: there's way more information here. I'm starting with the basic
pub struct RecordBatch {
    pub base_offset: u64,
    pub batch_length: u32, // how many bytes follow (including fields until records)
    pub records_count: u32,
    pub records: Bytes,
}

impl RecordBatch {
    pub fn get_size(&self) -> u32 {
        8 + self.batch_length
        // or 8 + 4 + 4 + self.records.len()
    }

    pub fn update_base_offset(&mut self, offset: u64) {
        self.base_offset = offset;
    }

    pub fn encode_header(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_u64(self.base_offset);
        buf.put_u32(self.batch_length);
        buf.put_u32(self.records_count);

        buf.freeze()
    }

    pub fn decode(buf: &[u8], pos: u64) -> Self {
        let mut header = [0u8; 16];
        header.copy_from_slice(&buf[pos as usize..pos as usize + 16]);

        let base_offset = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let batch_length = u32::from_le_bytes(header[8..12].try_into().unwrap());
        let records_count = u32::from_le_bytes(header[12..16].try_into().unwrap());

        let mut records = BytesMut::zeroed(batch_length as usize - 4);
        records.copy_from_slice(&buf[pos as usize + 16..pos as usize + 12 + batch_length as usize]); // ignore the extra 4 bytes in count

        Self {
            base_offset,
            batch_length,
            records_count,
            records: records.freeze(),
        }
    }

    // TODO: maybe change signature or reuse above implementation?
    pub fn decode_file(file: &File, pos: u64) -> Self {
        let mut header = [0u8; 16]; // 8 offset + 4 length + 4 count

        file.read_at(&mut header, pos).unwrap();

        let base_offset = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let batch_length = u32::from_le_bytes(header[8..12].try_into().unwrap());
        let records_count = u32::from_le_bytes(header[12..16].try_into().unwrap());

        let mut records = BytesMut::zeroed(batch_length as usize - 4);
        file.read_at(&mut records, pos + 16).unwrap();

        Self {
            base_offset,
            batch_length,
            records_count,
            records: records.freeze(),
        }
    }
}
