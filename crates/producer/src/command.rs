use crate::payload::PublishPayload;

pub enum ProducerCommand {
    Sync(PublishPayload),
    Async(PublishPayload),
    Shutdown,
}
