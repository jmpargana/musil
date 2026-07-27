use core::fmt;
use std::{
    io::{self, Write},
    ops::Deref,
    time::UNIX_EPOCH,
};

use bytes::{Buf, BufMut};

use crate::record_header::RecordHeader;

/**
 * Source: https://kafka.apache.org/43/implementation/message-format/
length: varint
attributes: int8
    bit 0~7: unused
timestampDelta: varlong
offsetDelta: varint
keyLength: varint
key: byte[]
valueLength: varint
value: byte[]
headersCount: varint
Headers => [Header]
 */
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Record {
    pub length: u32,
    pub attributes: u8, // TODO: bitmap
    // Initially this field is an actual timestamp. Later it turns into a delta
    // from the batch start (aka. base).
    pub timestamp_delta: u64,
    pub offset_delta: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub headers: Vec<RecordHeader>,
}

impl Record {
    pub fn new(offset_delta: u64, key: &[u8], value: &[u8]) -> Self {
        let mut record = Self {
            offset_delta,
            timestamp_delta: u64::from(UNIX_EPOCH.elapsed().unwrap().as_millis() as u64),
            key: key.to_vec(),
            value: value.to_vec(),
            length: 0,
            attributes: 0,
            headers: vec![],
        };
        record.length = record.get_size();
        record
    }

    pub(crate) fn get_size(&self) -> u32 {
        4 + 1
            + 8
            + 8
            + 4
            + self.key.len() as u32
            + 4
            + self.value.len() as u32
            + 4
            + self.headers.iter().map(|h| h.get_size()).sum::<u32>()
    }

    // TODO: actually nothing is really throwing here (for now).
    // Maybe I could simplify the error chaining.
    pub fn decode<B: Buf>(buf: &mut B) -> io::Result<Self> {
        let length = buf.get_u32();
        let attributes = buf.get_u8();
        let timestamp_delta = buf.get_u64();
        let offset_delta = buf.get_u64();
        let key_length = buf.get_u32() as usize;
        let key = buf.copy_to_bytes(key_length).deref().to_vec();
        let value_length = buf.get_u32() as usize;
        let value = buf.copy_to_bytes(value_length).deref().to_vec();

        let headers_size = buf.get_u32() as usize;
        let mut headers = Vec::with_capacity(headers_size);
        for _ in 0..headers_size {
            let header = RecordHeader::decode(buf)?;
            headers.push(header);
        }
        Ok(Self {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        })
    }

    // TODO: should return Result?
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        buf.put_u32(self.length);
        buf.put_u8(self.attributes);
        buf.put_u64(self.timestamp_delta);
        buf.put_u64(self.offset_delta);
        buf.put_u32(self.key.len() as u32);
        buf.put_slice(&self.key);
        buf.put_u32(self.value.len() as u32);
        buf.put_slice(&self.value);

        buf.put_u32(self.headers.len() as u32);
        for header in self.headers.iter() {
            header.encode(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::record::Record;

    #[test]
    fn decode_encode_e2e() {
        let record = Record::new(10, b"hello", b"world");
        let mut bytes = BytesMut::new();
        record.encode(&mut bytes);
        let decoded = Record::decode(&mut bytes).unwrap();
        assert_eq!(record, decoded);
    }
}
