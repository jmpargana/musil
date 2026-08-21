use derive_builder::Builder;
use serde::Deserialize;

use crate::replica::ReplicaMetadata;

#[derive(Builder, Clone, Debug, Deserialize)]
pub struct PartitionConfig {
    #[builder(default = 1<<20)]
    #[serde(default = "PartitionConfig::default_segment_bytes")]
    pub segment_bytes: usize,
    #[builder(default = 1<<16)]
    #[serde(default = "PartitionConfig::default_channel_size")]
    pub channel_size: usize,
    #[builder(default = vec![])]
    #[serde(skip)]
    pub replicas: Vec<ReplicaMetadata>,
    pub partition_id: u16,
    pub broker_id: u16,
    pub base_dir: String,
    pub topic_id: String,
}

impl PartitionConfig {
    fn default_segment_bytes() -> usize {
        1 << 20
    }
    fn default_channel_size() -> usize {
        1 << 16
    }
}
