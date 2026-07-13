use derive_builder::Builder;

// TODO: do I need Request or Response?
#[derive(Builder, Clone)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub api_version: u32,
    pub correlation_id: u32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    pub fn get_size(&self) -> u32 {
        let client_id_size = if let Some(c) = &self.client_id {
            c.len()
        } else {
            0
        };
        4 + 4 + 4 + 2 + client_id_size as u32
    }
}

// TODO: Actual size of payload is a combination of ApiKey and ApiVersion, but we'll ignore this for now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKey {
    Produce,
    Fetch,
    Metadata,
    // etc. add up to 40 (CreatePartitions)
}

#[derive(Debug)]
pub struct InvalidEnumValue(pub u32);

impl TryFrom<u32> for ApiKey {
    type Error = InvalidEnumValue;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ApiKey::Produce),
            1 => Ok(ApiKey::Fetch),
            3 => Ok(ApiKey::Metadata),
            _ => Err(InvalidEnumValue(value)),
        }
    }
}

impl From<ApiKey> for u32 {
    fn from(value: ApiKey) -> Self {
        match value {
            ApiKey::Produce => 0,
            ApiKey::Fetch => 1,
            ApiKey::Metadata => 3,
        }
    }
}
