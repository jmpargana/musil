#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataRecordType {
    Topic = 0,
    Partition = 1,
}

impl TryFrom<u8> for MetadataRecordType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Topic),
            1 => Ok(Self::Partition),
            other => Err(other),
        }
    }
}

pub enum MetadataRecord {
    Topic(TopicRecord),
    Partition(PartitionRecord),
}

pub struct TopicRecord {
    pub name: String,
}

pub struct PartitionRecord {
    pub topic_id: String,
    pub partition_id: u16,
    pub replicas: Vec<i32>,
    pub leader: i32,
}

impl MetadataRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        match self {
            MetadataRecord::Topic(t) => {
                buf.push(MetadataRecordType::Topic as u8);
                buf.extend_from_slice(&(t.name.len() as u16).to_be_bytes());
                buf.extend_from_slice(t.name.as_bytes());
            }
            MetadataRecord::Partition(p) => {
                buf.push(MetadataRecordType::Partition as u8);
                buf.extend_from_slice(&(p.topic_id.len() as u16).to_be_bytes());
                buf.extend_from_slice(p.topic_id.as_bytes());
                buf.extend_from_slice(&p.partition_id.to_be_bytes());
                buf.extend_from_slice(&(p.replicas.len() as u16).to_be_bytes());
                for r in &p.replicas {
                    buf.extend_from_slice(&r.to_be_bytes());
                }
                buf.extend_from_slice(&p.leader.to_be_bytes());
            }
        }
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.is_empty() {
            return None;
        }

        let kind = MetadataRecordType::try_from(buf[0]).ok()?;
        let buf = &buf[1..];

        match kind {
            MetadataRecordType::Topic => {
                let name_len = u16::from_be_bytes(buf[..2].try_into().ok()?) as usize;
                let name = String::from_utf8(buf[2..2 + name_len].to_vec()).ok()?;
                Some(MetadataRecord::Topic(TopicRecord { name }))
            }
            MetadataRecordType::Partition => {
                let mut pos = 0;
                let topic_id_len = u16::from_be_bytes(buf[pos..pos + 2].try_into().ok()?) as usize;
                pos += 2;
                let topic_id = String::from_utf8(buf[pos..pos + topic_id_len].to_vec()).ok()?;
                pos += topic_id_len;
                let partition_id = u16::from_be_bytes(buf[pos..pos + 2].try_into().ok()?);
                pos += 2;
                let replicas_len = u16::from_be_bytes(buf[pos..pos + 2].try_into().ok()?) as usize;
                pos += 2;
                let mut replicas = Vec::with_capacity(replicas_len);
                for _ in 0..replicas_len {
                    replicas.push(i32::from_be_bytes(buf[pos..pos + 4].try_into().ok()?));
                    pos += 4;
                }
                let leader = i32::from_be_bytes(buf[pos..pos + 4].try_into().ok()?);
                Some(MetadataRecord::Partition(PartitionRecord {
                    topic_id,
                    partition_id,
                    replicas,
                    leader,
                }))
            }
        }
    }
}
