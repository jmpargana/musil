use tokio::sync::oneshot;

use crate::storage::record::Record;

pub enum PartitionCommand {
    Append {
        record: Record,
        done: oneshot::Sender<()>,
    },
    UpdateReplicaLeo {
        replica_id: u32,
        leo: u64,
    },
    Shutdown,
}

pub enum ReplicaCommand {
    Replicate { record: Record },
    Shutdown,
}
