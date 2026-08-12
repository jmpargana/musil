use std::{fs::File, os::unix::fs::FileExt};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    batch_attributes::BatchAttributes,
    error::ProtoError,
    record::Record,
    record_iter::RecordIter,
    record_ref::{RecordRef, RecordRefIter},
};

pub const HEADER_SIZE: usize = 16; // base_offset(8) + batch_length(4) + records_count(4)

/// Bytes before the batch_length payload on disk: base_offset(8) + batch_length_field(4).
/// Total bytes on disk = BATCH_HEADER_PREFIX + batch_length.
pub const BATCH_HEADER_PREFIX: usize = 12;

// TODO: actually some values are ints instead of uints, but I don't understand why, even for -1 representations.
#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub base_offset: u64,
    pub batch_length: u32,
    pub partition_leader_epoch: i32,
    pub magic: u8,
    pub crc: u32,
    pub attributes: BatchAttributes,
    pub last_offset_delta: i32,
    pub base_timestamp: u64,
    pub max_timestamp: u64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records_count: u32,
    pub records: Bytes,
}

impl From<RecordBatch> for Vec<Record> {
    fn from(value: RecordBatch) -> Self {
        let mut records = value.records.clone(); // This clone is cheap, but it might not be the ideal call
        let mut ans = Vec::with_capacity(value.records_count as usize);
        for _ in 0..value.records_count {
            // FIXME: add error handler
            let record = Record::decode(&mut records).unwrap();
            ans.push(record);
        }
        ans
    }
}

impl From<Vec<Record>> for RecordBatch {
    fn from(value: Vec<Record>) -> Self {
        // Since this is called on creation, `base_offset` is always `0`.
        let base_offset = 0;
        let records_count = value.len() as u32;

        let mut buf = BytesMut::new();
        let mut base_timestamp = u64::MAX;
        let mut max_timestamp = 0;

        for r in value.iter() {
            if r.timestamp_delta < base_timestamp {
                base_timestamp = r.timestamp_delta;
            }
            if r.timestamp_delta > max_timestamp {
                max_timestamp = r.timestamp_delta;
            }
        }

        for (i, mut r) in value.into_iter().enumerate() {
            // TODO: change timestamp as well based on base
            r.offset_delta = i as u64;
            r.timestamp_delta = r.timestamp_delta - base_timestamp;
            r.encode_to(&mut buf);
        }

        let records = buf.freeze();
        // FIXME: compress with gzip
        let crc = crc_fast::crc32_iscsi(&records);

        // includes batch_length field.
        // TODO: find out if this breaks compatibility
        let batch_length = 4 + records.len() as u32;

        RecordBatch {
            base_offset,
            batch_length,
            crc,
            records_count,
            records,
            partition_leader_epoch: 0,
            magic: 2,                       // version
            attributes: BatchAttributes(0), // default gzip compression only for now
            last_offset_delta: records_count as i32 - 1,
            base_timestamp,
            max_timestamp,
            producer_id: 0, // TODO: ignoring for now
            producer_epoch: 0,
            base_sequence: 0, // FIXME: this will be needed when splitting in chunks (like TCP)
        }
    }
}

impl RecordBatch {
    /// Construct from the compact on-disk/wire format (base_offset + batch_length + records_count + records).
    /// All other fields get sensible defaults.
    pub fn from_compact(base_offset: u64, batch_length: u32, records_count: u32, records: Bytes) -> Self {
        let crc = crc_fast::crc32_iscsi(&records);
        Self {
            base_offset,
            batch_length,
            records_count,
            records,
            partition_leader_epoch: -1,
            magic: 2,
            crc,
            attributes: BatchAttributes(0),
            last_offset_delta: records_count as i32 - 1,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
        }
    }

    pub fn checksum(&self) -> bool {
        crc_fast::crc32_iscsi(&self.records) == self.crc
    }

    pub fn get_size(&self) -> u32 {
        BATCH_HEADER_PREFIX as u32 + self.batch_length
    }

    pub fn update_base_offset(&mut self, offset: u64) {
        self.base_offset = offset;
    }

    pub fn encode_header(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE);
        buf.extend_from_slice(&self.base_offset.to_be_bytes());
        buf.extend_from_slice(&self.batch_length.to_be_bytes());
        buf.extend_from_slice(&self.records_count.to_be_bytes());
        buf
    }

    pub fn decode_file(file: &File, pos: u64) -> Self {
        let mut header = [0u8; HEADER_SIZE];
        file.read_at(&mut header, pos).unwrap();

        let base_offset = u64::from_be_bytes(header[0..8].try_into().unwrap());
        let batch_length = u32::from_be_bytes(header[8..12].try_into().unwrap());
        let records_count = u32::from_be_bytes(header[12..16].try_into().unwrap());

        let records_len = batch_length as usize - 4;
        let mut records_buf = vec![0u8; records_len];
        file.read_at(&mut records_buf, pos + HEADER_SIZE as u64).unwrap();

        Self::from_compact(base_offset, batch_length, records_count, Bytes::from(records_buf))
    }

    /// Decode from a raw byte slice. `pos` is ignored (for API compat with tests).
    pub fn decode_slice(raw: &[u8], _pos: u64) -> Self {
        let base_offset = u64::from_be_bytes(raw[0..8].try_into().unwrap());
        let batch_length = u32::from_be_bytes(raw[8..12].try_into().unwrap());
        let records_count = u32::from_be_bytes(raw[12..16].try_into().unwrap());

        Self::from_compact(base_offset, batch_length, records_count, Bytes::copy_from_slice(&raw[16..]))
    }

    pub fn records_iter(&self) -> RecordIter {
        RecordIter::new(self.records.clone())
    }

    pub fn record_refs(&self) -> RecordRefIter {
        RecordRefIter::new(self.records.clone())
    }

    pub fn find_record(&self, offset_delta: u64) -> Option<RecordRef> {
        self.record_refs()
            .filter_map(|r| r.ok())
            .find(|r| r.offset_delta == offset_delta)
    }

    pub fn slice_from_offset(&self, offset: u64) -> Result<RecordBatch, ProtoError> {
        if offset == self.base_offset {
            return Ok(self.clone());
        }

        let delta = offset - self.base_offset;
        let loc = self.find_record(delta).ok_or(ProtoError::InvalidOffset)?;

        let sliced_records = self.records.slice(loc.byte_offset..);
        let records_count = self.records_count - loc.record_index();
        let batch_length = 4 + sliced_records.len() as u32;
        let crc = crc_fast::crc32_iscsi(&sliced_records);

        Ok(RecordBatch {
            base_offset: offset,
            batch_length,
            partition_leader_epoch: self.partition_leader_epoch,
            magic: self.magic,
            crc,
            attributes: self.attributes,
            last_offset_delta: self.last_offset_delta - delta as i32,
            base_timestamp: self.base_timestamp,
            max_timestamp: self.max_timestamp,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self.base_sequence,
            records_count,
            records: sliced_records,
        })
    }

    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        buf.put_u64(self.base_offset);
        buf.put_u32(self.batch_length);
        buf.put_i32(self.partition_leader_epoch);
        buf.put_u8(self.magic);
        buf.put_u32(self.crc);
        buf.put_u16(self.attributes.0);
        buf.put_i32(self.last_offset_delta);
        buf.put_u64(self.base_timestamp);
        buf.put_u64(self.max_timestamp);
        buf.put_i64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        buf.put_i32(self.base_sequence);
        buf.put_u32(self.records_count);
        buf.put_slice(&self.records);
    }

    pub fn decode(mut buf: Bytes) -> Result<Self, ProtoError> {
        let base_offset = buf.get_u64();
        let batch_length = buf.get_u32();
        let partition_leader_epoch = buf.get_i32();
        let magic = buf.get_u8();
        let crc = buf.get_u32();
        let attributes = buf.get_u16();
        let last_offset_delta = buf.get_i32();
        let base_timestamp = buf.get_u64();
        let max_timestamp = buf.get_u64();
        let producer_id = buf.get_i64();
        let producer_epoch = buf.get_i16();
        let base_sequence = buf.get_i32();
        let records_count = buf.get_u32();
        let records = buf;
        if crc_fast::crc32_iscsi(&records) != crc {
            return Err(ProtoError::CRC);
        };
        Ok(Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes: BatchAttributes(attributes),
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records_count,
            records: records.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;
    use std::io::Write;

    fn make_batch(base_offset: u64, records: &[Record]) -> RecordBatch {
        let records_count = records.len() as u32;
        let mut buf = BytesMut::new();
        for r in records {
            r.encode_to(&mut buf);
        }
        let encoded = buf.freeze();
        let batch_length = 4 + encoded.len() as u32;
        RecordBatch::from_compact(base_offset, batch_length, records_count, encoded)
    }

    fn encoded_batch_bytes(base_offset: u64, records: &[Record]) -> Vec<u8> {
        let mut enc = BytesMut::new();
        for r in records {
            r.encode_to(&mut enc);
        }
        let encoded = enc.freeze();
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
    fn get_size_equals_header_prefix_plus_batch_length() {
        let records = vec![Record::new(0, b"key", b"value")];
        let batch = make_batch(0, &records);
        assert_eq!(batch.get_size(), BATCH_HEADER_PREFIX as u32 + batch.batch_length);
    }

    #[test]
    fn get_size_matches_encode_header_plus_records() {
        let records = vec![Record::new(0, b"key", b"value")];
        let batch = make_batch(10, &records);
        let header = batch.encode_header();
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

        let batch = RecordBatch::decode_slice(&raw, 0);

        assert_eq!(batch.base_offset, 7);
        assert_eq!(batch.records_count, 1);

        let decoded = Record::decode(&mut batch.records.clone()).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn decode_file_matches_decode_slice() {
        let record = Record::new(0, b"file", b"record");
        let raw = encoded_batch_bytes(55, &[record.clone()]);

        let dir = std::env::temp_dir().join("rafka-proto-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("batch.bin");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&raw).unwrap();
        drop(f);

        let file = std::fs::File::open(&path).unwrap();
        let from_file = RecordBatch::decode_file(&file, 0);
        let from_slice = RecordBatch::decode_slice(&raw, 0);

        assert_eq!(from_file.base_offset, from_slice.base_offset);
        assert_eq!(from_file.batch_length, from_slice.batch_length);
        assert_eq!(from_file.records_count, from_slice.records_count);
        assert_eq!(from_file.records, from_slice.records);

        assert_eq!(Record::decode(&mut from_file.records.clone()).unwrap(), record);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn slice_from_offset_returns_clone_at_base() {
        let records = vec![
            Record::new(0, b"a", b"1"),
            Record::new(1, b"b", b"2"),
        ];
        let batch = make_batch(10, &records);
        let sliced = batch.slice_from_offset(10).unwrap();
        assert_eq!(sliced.records_count, 2);
        assert_eq!(sliced.base_offset, 10);
    }

    #[test]
    fn slice_from_offset_skips_first_record() {
        let records = vec![
            Record::new(0, b"a", b"1"),
            Record::new(1, b"b", b"2"),
            Record::new(2, b"c", b"3"),
        ];
        let batch = make_batch(10, &records);
        let sliced = batch.slice_from_offset(11).unwrap();
        assert_eq!(sliced.records_count, 2);
        assert_eq!(sliced.base_offset, 11);

        let decoded = Record::decode(&mut sliced.records.clone()).unwrap();
        assert_eq!(decoded.key, b"b");
    }

    #[test]
    fn slice_from_offset_invalid_returns_error() {
        let records = vec![Record::new(0, b"a", b"1")];
        let batch = make_batch(10, &records);
        assert!(batch.slice_from_offset(99).is_err());
    }

    #[test]
    fn find_record_returns_correct_ref() {
        let records = vec![
            Record::new(0, b"a", b"1"),
            Record::new(1, b"b", b"2"),
        ];
        let batch = make_batch(0, &records);
        let loc = batch.find_record(1).unwrap();
        assert_eq!(loc.offset_delta, 1);
        assert!(loc.byte_offset > 0);
    }
}
