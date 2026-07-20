use crate::protocol::fetch::request::fetch_topic::FetchTopic;

#[derive(Debug)]
pub struct FetchRequest {
    pub replica_id: i32,
    pub max_bytes: u32,
    pub topics: Vec<FetchTopic>,
}
