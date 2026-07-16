use std::{collections::HashMap, sync::Arc};

use crate::{
    broker::metadata_record::{PartitionRecord, TopicRecord},
    partition::{config::PartitionConfigBuilder, handle::PartitionHandle},
    topic::TopicPartition,
};

#[derive(Clone)]
pub struct MetadataImage {
    pub partitions: HashMap<TopicPartition, Arc<PartitionHandle>>,
}

impl MetadataImage {
    pub fn empty() -> Self {
        MetadataImage {
            partitions: HashMap::new(),
        }
    }

    pub fn create_topic(
        mut self,
        path: &str,
        topic: &TopicRecord,
        partition_records: &[PartitionRecord],
    ) -> Self {
        for p in partition_records {
            let config = PartitionConfigBuilder::default()
                .base_dir(path.to_string())
                .partition_id(p.partition_id)
                .broker_id(p.leader as u16)
                .topic_id(topic.name.clone())
                .build()
                .expect("topic config to be correct");

            let handle = PartitionHandle::spawn(p.partition_id as u32, config);

            self.partitions.insert(
                TopicPartition {
                    topic_id: topic.name.clone(),
                    partition_id: p.partition_id,
                },
                handle,
            );
        }
        self
    }
}
