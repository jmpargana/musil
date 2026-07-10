use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use tokio::{
    sync::{
        mpsc::{self, channel, error::SendError},
        oneshot,
    },
    task::JoinHandle,
};

use crate::{
    partition::{
        actor::PartitionActor,
        command::PartitionCommand,
        config::PartitionConfig,
        state::PartitionState,
    },
    protocol::fetch::{
        request::fetch_partition::FetchPartition,
        response::partition_response::PartitionResponse,
    },
    segment::metadata::RecordLocation,
    storage::record::Record,
};

use std::path::Path;

pub struct PartitionHandle {
    id: u32,
    tx: mpsc::Sender<PartitionCommand>,
    pub state: Arc<ArcSwap<PartitionState>>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl PartitionHandle {
    // TODO: handle replicas
    pub fn spawn(
        topic_id: String,
        id: u32,
        base_dir: String,
        config: PartitionConfig,
    ) -> Arc<Self> {
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
        let mut actor =
            PartitionActor::new(rx, base_dir, config.segment_bytes, state.clone()).unwrap();
        let join = tokio::spawn(async move {
            actor.run().await;
        });

        Arc::new(Self {
            id,
            tx,
            state,
            join: Mutex::new(Some(join)),
        })
    }

    pub async fn send(&self, c: PartitionCommand) -> Result<(), SendError<PartitionCommand>> {
        self.tx.send(c).await
    }

    // TODO: respond based on ack
    pub async fn append(&self, record: Record) {
        let (tx, rx) = oneshot::channel();
        self.send(PartitionCommand::Append { record, done: tx })
            .await
            .unwrap();
        rx.await.unwrap();
    }

    pub fn find_pos(&self, offset: u64) -> Option<RecordLocation> {
        self.state.load_full().find_pos(offset)
    }

    pub async fn fetch(&self, fetch_req: &FetchPartition, replica_id: i32) -> PartitionResponse {
        let res = self.state.load_full().fetch(self.id, fetch_req);

        if replica_id >= 0 {
            self.send(PartitionCommand::UpdateReplicaLeo {
                replica_id: replica_id as u32,
                leo: res.log_start_offset,
            })
            .await
            .unwrap();
        }

        res
    }

    pub async fn shutdown(&self) {
        self.send(PartitionCommand::Shutdown).await.unwrap();
        if let Some(join) = self.join.lock().unwrap().take() {
            join.await.unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::config::PartitionConfigBuilder;
    use crate::storage::record::Record;

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
        let handle = PartitionHandle::spawn("test".to_string(), 0, dir, cfg);
        let record = Record::new(b"hello", b"world");

        let offset = handle.find_pos(1);
        assert!(offset.is_none());

        handle.append(record).await;

        let offset = handle.find_pos(1);
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
        let handle = PartitionHandle::spawn("test".to_string(), 0, dir, cfg);
        let record = Record::new(b"hello", b"world");

        let offset = handle.find_pos(1);
        assert!(offset.is_none());

        handle.append(record).await;

        let state = handle.state.load_full();
        assert_eq!(state.segments.len(), 2);
    }
}
