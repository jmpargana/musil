use tokio::sync::oneshot;

use network::protocol::metadata::{CreateTopicRequest, CreateTopicResponse};

pub enum MetadataCommand {
    CreateTopic {
        req: CreateTopicRequest,
        done: oneshot::Sender<CreateTopicResponse>,
    },
    AddPartition {},
    RegisterBroker {},
}
