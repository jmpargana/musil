use proto::record::Record;

#[derive(Debug)]
pub struct PublishPayload {
    pub topic: String,
    pub key: Option<String>,
    pub value: String,
}

impl From<PublishPayload> for Record {
    fn from(value: PublishPayload) -> Self {
        let key = value.key.as_deref().unwrap_or("").as_bytes();
        let val = value.value.as_bytes();
        Record::new(0, key, val)
    }
}
