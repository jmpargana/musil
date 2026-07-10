use std::{path::Path, sync::Arc};

use crate::{
    partition::{
        actor::PartitionActor,
        command::PartitionCommand,
        config::PartitionConfig,
        handle::PartitionHandle,
        state::PartitionState,
    },
    protocol::fetch::{
        request::fetch_partition::FetchPartition,
        response::partition_response::PartitionResponse,
    },
    segment::metadata::RecordLocation,
    storage::record::Record,
};

use arc_swap::ArcSwap;
use tokio::sync::{mpsc::channel, oneshot};

pub mod actor;
pub mod command;
pub mod config;
pub mod handle;
pub mod state;

// TODO: this struct might be removed all together. It's enough to create a handle
pub struct Partition {
    id: u32,
    handle: PartitionHandle,
    join: tokio::task::JoinHandle<()>,
}

impl Partition {
    async fn shutdown(self) {
        self.handle.send(PartitionCommand::Shutdown).await.unwrap();
        self.join.await.unwrap();
    }

    // TODO: respond based on ack
    pub async fn append(&self, record: Record) {
        let (tx, rx) = oneshot::channel();
        self.handle
            .send(PartitionCommand::Append { record, done: tx })
            .await
            .unwrap();
        rx.await.unwrap();
    }

    pub fn find_pos(&self, offset: u64) -> Option<RecordLocation> {
        self.handle.find(offset)
    }

    // TODO: handle replicas
    fn with_config(topic_id: String, id: u32, base_dir: String, config: PartitionConfig) -> Self {
        let base_path = Path::new(&base_dir);
        let topic_partition_name = format!("{}-{}", topic_id, id);
        let base_dir = base_path
            .join(topic_partition_name)
            .as_path()
            .to_str()
            .unwrap()
            .to_string();

        let (tx, rx) = channel(config.channel_size);
        let state = Arc::new(ArcSwap::from_pointee(PartitionState::new(config.replicas)));
        let mut writer =
            PartitionActor::new(rx, base_dir, config.segment_bytes, state.clone()).unwrap();
        let join = tokio::spawn(async move {
            writer.run().await;
        });
        Self {
            id,
            handle: PartitionHandle::new(tx, state.clone()),
            join,
        }
    }

    pub(crate) async fn fetch(
        &self,
        fetch_req: &FetchPartition,
        replica_id: i32,
    ) -> PartitionResponse {
        let res = self.handle.fetch(self.id, fetch_req);

        if replica_id >= 0 {
            self.handle
                .send(PartitionCommand::UpdateReplicaLeo {
                    replica_id: replica_id as u32,
                    leo: res.log_start_offset,
                })
                .await
                .unwrap();
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::config::PartitionConfigBuilder;

    use super::*;

    #[tokio::test]
    async fn write_reads() {
        let dir = tempdir::TempDir::new("./")
            .unwrap()
            .path()
            .to_str()
            .unwrap()
            .to_string();
        let cfg = PartitionConfigBuilder::default().build().unwrap();
        let partition = Partition::with_config("test".to_string(), 0, dir, cfg);
        let record = Record::new(b"hello", b"world");

        let offset = partition.find_pos(1);
        assert!(offset.is_none());

        partition.append(record).await;

        let offset = partition.find_pos(1);
        assert!(offset.is_some());
    }

    #[tokio::test]
    async fn giant_record_creates_new_segment() {
        let dir = tempdir::TempDir::new("./")
            .unwrap()
            .path()
            .to_str()
            .unwrap()
            .to_string();
        let cfg = PartitionConfigBuilder::default()
            .segment_bytes(3)
            .build()
            .unwrap();
        let partition = Partition::with_config("test".to_string(), 0, dir, cfg);
        let record = Record::new(b"hello", b"world");

        let offset = partition.find_pos(1);
        assert!(offset.is_none());

        partition.append(record).await;

        let state = partition.handle.state.load_full();
        assert_eq!(state.segments.len(), 2);
    }
}
