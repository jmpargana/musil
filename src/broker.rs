use crate::topic::Topic;

pub struct Broker {
    topics: dashmap::DashMap<String, Topic>
}