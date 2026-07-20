use proto::record::Record;

pub struct PublishPayload {
    pub topic: String,
    pub key: Option<String>,
    // FIXME: maybe I can take Bytes already as input?
    pub value: String,
}

impl From<PublishPayload> for Record {
    fn from(value: PublishPayload) -> Self {
        // offset_delta get's populated by RecordBatch From. Same as timestamp.
        // TODO: timestamp not being used correctly
        Record {
            offset_delta: 0,
            timestamp: 0,
            key: value.key.as_deref().unwrap_or("").as_bytes().to_vec(),
            value: value.value.as_bytes().to_vec(),
        }
    }
}
