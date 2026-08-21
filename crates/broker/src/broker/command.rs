use network::protocol::metadata::{CreateTopicRequest, CreateTopicResponse};
use tokio::sync::oneshot;

pub enum MetadataCommand {
    CreateTopic {
        req: CreateTopicRequest,
        done: oneshot::Sender<CreateTopicResponse>,
    },
    AddPartition {},
    RegisterBroker {},
}
