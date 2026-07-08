use tokio::sync::oneshot;

use crate::{batch::Batch, record::Record};

// TODO: needs to be refactored into events for specific domains:
// - partitionactor
// - replicationactor
// - etc.
pub enum Command {
    Append {
        record: Record,
        done: oneshot::Sender<()>,
    },
    AppendV2 {
        batch: Batch,
        done: oneshot::Sender<()>,
    },
    Fetch {
        
    },
    ReplicaAck {
        broker_id: String,
        offset: u64,
        done: oneshot::Sender<()>,
    },
    ReplicaRequest {
        broker_id: String,
        record: Record,
    },
    Shutdown,
}
