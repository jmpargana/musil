use std::{io::{self, Write}, time::{UNIX_EPOCH}};


#[derive(PartialEq, Eq, Debug)]
pub struct Record {
    pub offset: Option<u64>, // initially set to None, until partition handler assigns
    pub timestamp: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>
}

impl Record {
    pub fn new(key: &[u8], value: &[u8]) -> Self {
        Self {
            offset: None,
            timestamp: UNIX_EPOCH.elapsed().unwrap().as_secs(), // probably need different way to get ms accuracy
            key: key.to_vec(),
            value: value.to_vec()
        }
    } 

    pub fn add_offset(&mut self, offset: u64) {
        self.offset = Some(offset);
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1<<12);
        self.write_to(&mut buf).unwrap();
        buf
    }
    
    pub fn decode(buf: &[u8]) -> io::Result<Self> {
        let mut pos = 0;
        
        let offset = u64::from_le_bytes(buf[pos..pos+8].try_into().unwrap());
        pos += 8;

        let timestamp = u64::from_le_bytes(buf[pos..pos+8].try_into().unwrap());
        pos += 8;

        let key_size = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize;
        pos += 4;
        
        let key = buf[pos..pos+key_size].to_vec();
        pos += key_size;

        let value_size = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize;
        pos += 4;
        
        let value = buf[pos..pos+value_size].to_vec();

        Ok(Record {
            offset: Some(offset),
            timestamp,
            key,
            value            
        })
    }
    
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        let mut size: usize = 0;

        let temp = &self.offset.unwrap().to_le_bytes(); // must be Some by now
        writer.write_all(temp);
        size += temp.len();

        let temp = &self.timestamp.to_le_bytes();
        writer.write_all(temp);
        size += temp.len();

        let temp = &(self.key.len() as u32).to_le_bytes();
        writer.write_all(temp);
        size += temp.len();
        writer.write_all(&self.key);
        size += self.key.len();

        let temp = &(self.value.len() as u32).to_le_bytes();
        writer.write_all(temp);
        size += temp.len();
        writer.write_all(&self.value);
        size += self.value.len();
        
        Ok(size)
    }
    
}

#[cfg(test)]
mod tests {
    use crate::record::Record;

    #[test]
    fn decode_encode_e2e() {
        let mut record = Record::new(b"hello", b"world");
        record.add_offset(1);
        
        let bytes = record.encode();
        let decoded = Record::decode(&bytes).unwrap();
        assert_eq!(record, decoded);
    }
}