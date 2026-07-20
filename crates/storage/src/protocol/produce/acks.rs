use crate::protocol::header::InvalidEnumValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    None,
    Leader,
    All,
}

impl TryFrom<u32> for Acks {
    type Error = InvalidEnumValue;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Acks::None),
            1 => Ok(Acks::Leader),
            2 => Ok(Acks::All),
            _ => Err(InvalidEnumValue(value)),
        }
    }
}

impl From<Acks> for u32 {
    fn from(value: Acks) -> Self {
        match value {
            Acks::None => 0,
            Acks::Leader => 1,
            Acks::All => 2,
        }
    }
}
