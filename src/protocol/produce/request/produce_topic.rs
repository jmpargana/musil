use crate::protocol::produce::request::produce_partition::ProducePartition;

pub struct ProduceTopic {
    pub topic: String,
    pub partitions: Vec<ProducePartition>,
}
