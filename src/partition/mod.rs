use std::{path::Path, sync::Arc};

use crate::{
    batch::Batch,
    command::Command,
    partition::{
        actor::PartitionActor, config::PartitionConfig, handle::PartitionHandle,
        state::PartitionState,
    },
    record::Record,
    segment::metadata::RecordLocation,
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use tokio::sync::{
    mpsc::{self, channel},
    oneshot,
};

pub mod actor;
pub mod config;
pub mod handle;
pub mod state;

pub struct Partition {
    id: u32,
    handle: PartitionHandle,
    join: tokio::task::JoinHandle<()>,
}

impl Partition {
    async fn shutdown(self) {
        self.handle.send(Command::Shutdown).await.unwrap();
        self.join.await.unwrap();
    }

    pub async fn produce(&self, batch: Bytes) {}

    // TODO: respond based on ack
    pub async fn append(&self, record: Record) {
        let (tx, rx) = oneshot::channel();
        self.handle
            .send(Command::Append { record, done: tx })
            .await
            .unwrap();
        rx.await.unwrap();
    }

    pub fn find_pos(&self, offset: u64) -> Option<RecordLocation> {
        self.handle.find(offset)
    }

    // TODO: handle replicas
    fn with_config(
        topic_id: String,
        id: u32,
        base_dir: String,
        replication_tx: mpsc::Sender<Command>,
        config: PartitionConfig,
    ) -> Self {
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
        let mut writer = PartitionActor::new(
            rx,
            base_dir,
            config.segment_bytes,
            state.clone(),
            replication_tx,
        )
        .unwrap();
        let join = tokio::spawn(async move {
            writer.run().await;
        });
        Self {
            id,
            handle: PartitionHandle::new(tx, state.clone()),
            join,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{partition::config::PartitionConfigBuilder, replica::ReplicaMetadata};

    use super::*;

    #[tokio::test]
    async fn write_reads() {
        let (tx, _) = mpsc::channel(1);
        let dir = tempdir::TempDir::new("./")
            .unwrap()
            .path()
            .to_str()
            .unwrap()
            .to_string();
        let cfg = PartitionConfigBuilder::default().build().unwrap();
        let partition = Partition::with_config("test".to_string(), 0, dir, tx, cfg);
        let record = Record::new(b"hello", b"world");

        let offset = partition.find_pos(1);
        assert!(offset.is_none());

        partition.append(record).await;

        let offset = partition.find_pos(1);
        assert!(offset.is_some());
    }

    #[tokio::test]
    async fn giant_record_creates_new_segment() {
        let (tx, _) = mpsc::channel(1);
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
        let partition = Partition::with_config("test".to_string(), 0, dir, tx, cfg);
        let record = Record::new(b"hello", b"world");

        let offset = partition.find_pos(1);
        assert!(offset.is_none());

        partition.append(record).await;

        let state = partition.handle.state.load_full();
        assert_eq!(state.segments.len(), 2);
    }

    #[tokio::test]
    async fn calls_each_replica() {
        let (tx, mut rx) = mpsc::channel(2);
        let dir = tempdir::TempDir::new("./")
            .unwrap()
            .path()
            .to_str()
            .unwrap()
            .to_string();
        let cfg = PartitionConfigBuilder::default()
            .segment_bytes(3)
            .replicas(vec![
                ReplicaMetadata::empty("1".to_string()),
                ReplicaMetadata::empty("2".to_string()),
            ])
            .build()
            .unwrap();
        let partition = Partition::with_config("test".to_string(), 0, dir, tx, cfg);
        let record = Record::new(b"hello", b"world");

        let offset = partition.find_pos(1);
        assert!(offset.is_none());

        partition.append(record).await;

        let mut received = Vec::new();

        for _ in 0..2 {
            let cmd = rx.recv().await.unwrap();
            let Command::ReplicaRequest { broker_id, .. } = cmd else {
                panic!("expecterd ReplicaRequest");
            };
            received.push(broker_id);
        }

        assert_eq!(received.len(), 2);
        received.sort();
        assert_eq!(received[0], "1".to_string());
        assert_eq!(received[1], "2".to_string());
    }

    #[tokio::test]
    async fn high_watermark_updates_after_all() {
        let (tx, mut rx) = mpsc::channel(2);
        let dir = tempdir::TempDir::new("./")
            .unwrap()
            .path()
            .to_str()
            .unwrap()
            .to_string();
        let cfg = PartitionConfigBuilder::default()
            .segment_bytes(3)
            .replicas(vec![
                ReplicaMetadata::empty("1".to_string()),
                ReplicaMetadata::empty("2".to_string()),
            ])
            .build()
            .unwrap();
        let partition = Partition::with_config("test".to_string(), 0, dir, tx, cfg);
        let record = Record::new(b"hello", b"world");

        let offset = partition.find_pos(1);
        assert!(offset.is_none());

        partition.append(record).await;

        for _ in 0..2 {
            let cmd = rx.recv().await.unwrap();
            let Command::ReplicaRequest { .. } = cmd else {
                panic!("expecterd ReplicaRequest");
            };
        }

        let state = partition.handle.state.load_full();

        assert_eq!(state.high_watermark, 0);

        let (tx, rx) = oneshot::channel();

        partition
            .handle
            .send(Command::ReplicaAck {
                broker_id: "1".to_string(),
                offset: 1,
                done: tx,
            })
            .await
            .unwrap();

        rx.await.unwrap();
        let state = partition.handle.state.load_full();

        assert_eq!(state.high_watermark, 0);

        let (tx, rx) = oneshot::channel();
        partition
            .handle
            .send(Command::ReplicaAck {
                broker_id: "2".to_string(),
                offset: 1,
                done: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap();

        let state = partition.handle.state.load_full();
        assert_eq!(state.high_watermark, 1);
    }
}
