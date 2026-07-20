use crate::protocol::fetch::request::fetch_partition::FetchPartition;

#[derive(Debug)]
pub struct FetchTopic {
    pub topic: String,
    pub partitions: Vec<FetchPartition>,
}
