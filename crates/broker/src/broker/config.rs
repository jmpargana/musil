use derive_builder::Builder;
use network::protocol::metadata::BrokerMetadata;
use serde::Deserialize;
use storage::partition::config::PartitionConfig;

#[derive(Builder, Clone, Debug, Deserialize)]
pub struct BrokerConfig {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub topics: Vec<TopicConfig>,
}

impl From<&BrokerConfig> for BrokerMetadata {
    fn from(config: &BrokerConfig) -> Self {
        Self {
            node_id: config.node_id,
            host: config.host.to_string(),
            port: config.port,
        }
    }
}

#[derive(Builder, Clone, Debug, Deserialize)]
pub struct TopicConfig {
    pub partitions: Vec<PartitionConfig>,
}
