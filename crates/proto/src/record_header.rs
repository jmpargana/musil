use std::ops::Deref;

use bytes::{Buf, BufMut};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RecordHeader {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl RecordHeader {
    pub fn decode<B: Buf>(buf: &mut B) -> std::io::Result<Self> {
        let key_len = buf.get_u32() as usize;
        let key = buf.copy_to_bytes(key_len).deref().to_vec();
        let value_len = buf.get_u32() as usize;
        let value = buf.copy_to_bytes(value_len).deref().to_vec();
        Ok(Self { key, value })
    }

    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        buf.put_u32(self.key.len() as u32);
        buf.put_slice(&self.key);
        buf.put_u32(self.value.len() as u32);
        buf.put_slice(&self.value);
    }

    pub(crate) fn get_size(&self) -> u32 {
        4 + self.key.len() as u32 + 4 + self.value.len() as u32
    }
}
