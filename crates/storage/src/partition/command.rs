use tokio::sync::oneshot;

use proto::{
    produce::{acks::Acks, response::partition_response::ProducePartitionResponse},
    record::Record,
    record_batch::RecordBatch,
};

pub enum PartitionCommand {
    Append {
        record: RecordBatch,
        acks: Acks,
        done: oneshot::Sender<ProducePartitionResponse>,
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
