use crate::message::header::InvalidEnumValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ack {
    None,
    Leader,
    All,
}

impl TryFrom<u32> for Ack {
    type Error = InvalidEnumValue;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Ack::None),
            1 => Ok(Ack::Leader),
            2 => Ok(Ack::All),
            _ => Err(InvalidEnumValue(value)),
        }
    }
}
