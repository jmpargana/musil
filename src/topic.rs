
#[derive(Eq, PartialEq, Hash, Clone)]
pub struct TopicPartition {
    pub topic_id: String,
    pub partition_id: u16,
}
