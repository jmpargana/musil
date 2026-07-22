use core::fmt;
use std::{
    io::{self, Write},
    time::UNIX_EPOCH,
};

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Record {
    pub offset_delta: u64,
    pub timestamp: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    pub fn new(offset_delta: u64, key: &[u8], value: &[u8]) -> Self {
        Self {
            offset_delta,
            timestamp: UNIX_EPOCH.elapsed().unwrap().as_secs(),
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 << 12);
        self.write_to(&mut buf).unwrap();
        buf
    }

    pub fn decode(buf: &[u8]) -> io::Result<Record> {
        Record::decode_raw(buf).map(|it| it.0)
    }

    pub fn decode_raw(buf: &[u8]) -> io::Result<(Record, usize)> {
        let mut pos = 0;

        let offset_delta = u64::from_be_bytes(buf[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let timestamp = u64::from_be_bytes(buf[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let key_size = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let key = buf[pos..pos + key_size].to_vec();
        pos += key_size;

        let value_size = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let value = buf[pos..pos + value_size].to_vec();
        pos += value_size;

        Ok((
            Record {
                offset_delta,
                timestamp,
                key,
                value,
            },
            pos,
        ))
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        let mut size: usize = 0;

        let temp = &self.offset_delta.to_be_bytes();
        writer.write_all(temp)?;
        size += temp.len();

        let temp = &self.timestamp.to_be_bytes();
        writer.write_all(temp)?;
        size += temp.len();

        let temp = &(self.key.len() as u32).to_be_bytes();
        writer.write_all(temp)?;
        size += temp.len();
        writer.write_all(&self.key)?;
        size += self.key.len();

        let temp = &(self.value.len() as u32).to_be_bytes();
        writer.write_all(temp)?;
        size += temp.len();
        writer.write_all(&self.value)?;
        size += self.value.len();

        Ok(size)
    }

    pub(crate) fn get_size(&self) -> usize {
        8 + 8 + 4 + 4 + self.key.len() + self.value.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::record::Record;

    #[test]
    fn decode_encode_e2e() {
        let record = Record::new(10, b"hello", b"world");
        let bytes = record.encode();
        let decoded = Record::decode(&bytes).unwrap();
        assert_eq!(record, decoded);
    }
}
