const HEADER_SIZE: usize = 16; // 4xu32

// TODO: do I need Request or Response?
pub struct MessageHeader {
    pub api_key: MessageApiKey,
    pub api_version: u32,
    pub correlation_id: u32,
    pub client_id: Option<String>,
}

// TODO: Actual size of payload is a combination of ApiKey and ApiVersion, but we'll ignore this for now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageApiKey {
    Produce,
    Fetch,
    // other values:
    // - Metadata
    // - ApiVersions
    // - SaslAuthenticate
}

#[derive(Debug)]
pub struct InvalidEnumValue(pub u32);

impl TryFrom<u32> for MessageApiKey {
    type Error = InvalidEnumValue;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MessageApiKey::Produce),
            1 => Ok(MessageApiKey::Fetch),
            _ => Err(InvalidEnumValue(value)),
        }
    }
}
