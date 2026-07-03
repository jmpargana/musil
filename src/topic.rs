use crate::partition::{Partition, handle::PartitionHandle};

pub struct Topic {
    id: String,
    // TODO: might need an Arc here
    partitions: Vec<Partition>,
}
