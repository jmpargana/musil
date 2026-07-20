use crate::protocol::produce::request::produce_partition::ProducePartition;

#[derive(Debug)]
pub struct ProduceTopic {
    pub topic: String,
    pub partitions: Vec<ProducePartition>,
}
