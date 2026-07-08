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

impl From<Ack> for u32 {
    fn from(value: Ack) -> Self {
        match value {
            Ack::None => 0,
            Ack::Leader => 1,
            Ack::All => 2,
        }
    }
}
