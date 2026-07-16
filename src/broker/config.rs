use derive_builder::Builder;
use serde::Deserialize;

use crate::{partition::config::PartitionConfig, protocol::metadata::BrokerMetadata};

#[derive(Builder, Clone, Debug, Deserialize)]
pub struct BrokerConfig {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub topics: Vec<TopicConfig>,
}

impl Into<BrokerMetadata> for &BrokerConfig {
    fn into(self) -> BrokerMetadata {
        BrokerMetadata {
            node_id: self.node_id,
            host: self.host.to_string(),
            port: self.port,
        }
    }
}

#[derive(Builder, Clone, Debug, Deserialize)]
pub struct TopicConfig {
    pub partitions: Vec<PartitionConfig>,
}
