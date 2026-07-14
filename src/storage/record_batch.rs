use std::{fs::File, os::unix::fs::FileExt};

use bytes::{BufMut, Bytes, BytesMut};

// TODO: there's way more information here. I'm starting with the basic
#[derive(Debug)]
pub struct RecordBatch {
    pub base_offset: u64,
    pub batch_length: u32, // how many bytes follow (including fields until records)
    pub records_count: u32,
    pub records: Bytes,
}

impl RecordBatch {
    pub fn get_size(&self) -> u32 {
        // base_offset(8) + batch_length_field(4) + batch_length bytes = 12 + batch_length
        12 + self.batch_length
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

        let base_offset = u64::from_be_bytes(header[0..8].try_into().unwrap());
        let batch_length = u32::from_be_bytes(header[8..12].try_into().unwrap());
        let records_count = u32::from_be_bytes(header[12..16].try_into().unwrap());

        let mut records = BytesMut::zeroed(batch_length as usize - 4);
        records.copy_from_slice(&buf[pos as usize + 16..pos as usize + 12 + batch_length as usize]); // ignore the extra 4 bytes in count

        Self {
            base_offset,
            batch_length,
            records_count,
            records: records.freeze(),
        }
    }

    pub fn decode_file(file: &File, pos: u64) -> Self {
        let mut header = [0u8; 16]; // 8 offset + 4 length + 4 count

        file.read_at(&mut header, pos).unwrap();

        let base_offset = u64::from_be_bytes(header[0..8].try_into().unwrap());
        let batch_length = u32::from_be_bytes(header[8..12].try_into().unwrap());
        let records_count = u32::from_be_bytes(header[12..16].try_into().unwrap());

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::Record;
    use std::io::Write;
    use tempdir::TempDir;

    fn make_batch(base_offset: u64, records: &[Record]) -> RecordBatch {
        let records_count = records.len() as u32;
        let encoded: Vec<u8> = records.iter().flat_map(|r| r.encode()).collect();
        let batch_length = 4 + encoded.len() as u32;
        RecordBatch {
            base_offset,
            batch_length,
            records_count,
            records: Bytes::from(encoded),
        }
    }

    fn encoded_batch_bytes(base_offset: u64, records: &[Record]) -> Vec<u8> {
        let encoded: Vec<u8> = records.iter().flat_map(|r| r.encode()).collect();
        let records_count = records.len() as u32;
        let batch_length = 4 + encoded.len() as u32;

        let mut buf = Vec::new();
        buf.extend_from_slice(&base_offset.to_be_bytes());
        buf.extend_from_slice(&batch_length.to_be_bytes());
        buf.extend_from_slice(&records_count.to_be_bytes());
        buf.extend_from_slice(&encoded);
        buf
    }

    #[test]
    fn get_size_equals_12_plus_batch_length() {
        let records = vec![Record::new(0, b"key", b"value")];
        let batch = make_batch(0, &records);
        // base_offset(8) + batch_length_field(4) + batch_length bytes
        assert_eq!(batch.get_size(), 12 + batch.batch_length);
    }

    #[test]
    fn get_size_matches_encode_header_plus_records() {
        let records = vec![Record::new(0, b"key", b"value")];
        let batch = make_batch(10, &records);
        let header = batch.encode_header(); // 16 bytes
        let wire_len = (header.len() + batch.records.len()) as u32;
        assert_eq!(batch.get_size(), wire_len);
    }

    #[test]
    fn update_base_offset() {
        let records = vec![Record::new(0, b"k", b"v")];
        let mut batch = make_batch(0, &records);
        batch.update_base_offset(42);
        assert_eq!(batch.base_offset, 42);
    }

    #[test]
    fn encode_header_roundtrip() {
        let records = vec![Record::new(0, b"hello", b"world")];
        let batch = make_batch(100, &records);
        let encoded = batch.encode_header();
        assert_eq!(encoded.len(), 16);

        let base_offset = u64::from_be_bytes(encoded[0..8].try_into().unwrap());
        let batch_length = u32::from_be_bytes(encoded[8..12].try_into().unwrap());
        let records_count = u32::from_be_bytes(encoded[12..16].try_into().unwrap());

        assert_eq!(base_offset, batch.base_offset);
        assert_eq!(batch_length, batch.batch_length);
        assert_eq!(records_count, batch.records_count);
    }

    #[test]
    fn decode_from_slice_single_record() {
        let record = Record::new(0, b"key", b"value");
        let raw = encoded_batch_bytes(7, &[record.clone()]);

        let batch = RecordBatch::decode(&raw, 0);

        assert_eq!(batch.base_offset, 7);
        assert_eq!(batch.records_count, 1);

        let decoded = Record::decode(&batch.records).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn decode_from_slice_multiple_records() {
        let records = vec![
            Record::new(0, b"k1", b"v1"),
            Record::new(1, b"k2", b"v2"),
            Record::new(2, b"k3", b"v3"),
        ];
        let raw = encoded_batch_bytes(10, &records);

        let batch = RecordBatch::decode(&raw, 0);

        assert_eq!(batch.base_offset, 10);
        assert_eq!(batch.records_count, 3);

        let mut pos = 0;
        for expected in &records {
            let (decoded, consumed) = Record::decode_raw(&batch.records[pos..]).unwrap();
            assert_eq!(decoded, *expected);
            pos += consumed + expected.encode().len() - consumed; // advance by full record size
        }
    }

    #[test]
    fn decode_from_slice_with_pos_offset() {
        let record = Record::new(0, b"off", b"set");
        let mut raw = vec![0xFFu8; 20];
        raw.extend(encoded_batch_bytes(99, &[record.clone()]));

        let batch = RecordBatch::decode(&raw, 20);

        assert_eq!(batch.base_offset, 99);
        assert_eq!(Record::decode(&batch.records).unwrap(), record);
    }

    #[test]
    fn decode_file_matches_decode_slice() {
        let record = Record::new(0, b"file", b"record");
        let raw = encoded_batch_bytes(55, &[record.clone()]);

        let tmp = TempDir::new("rafka-test").unwrap();
        let path = tmp.path().join("batch.bin");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&raw).unwrap();
        drop(f);

        let file = std::fs::File::open(&path).unwrap();
        let from_file = RecordBatch::decode_file(&file, 0);
        let from_slice = RecordBatch::decode(&raw, 0);

        assert_eq!(from_file.base_offset, from_slice.base_offset);
        assert_eq!(from_file.batch_length, from_slice.batch_length);
        assert_eq!(from_file.records_count, from_slice.records_count);
        assert_eq!(from_file.records, from_slice.records);

        assert_eq!(Record::decode(&from_file.records).unwrap(), record);
    }

    #[test]
    fn decode_file_with_pos_offset() {
        let record = Record::new(0, b"offset", b"data");
        let padding = vec![0xABu8; 32];
        let mut raw = padding;
        raw.extend(encoded_batch_bytes(11, &[record.clone()]));

        let tmp = TempDir::new("rafka-test").unwrap();
        let path = tmp.path().join("batch_offset.bin");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&raw).unwrap();
        drop(f);

        let file = std::fs::File::open(&path).unwrap();
        let batch = RecordBatch::decode_file(&file, 32);

        assert_eq!(batch.base_offset, 11);
        assert_eq!(Record::decode(&batch.records).unwrap(), record);
    }
}
