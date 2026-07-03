use std::sync::Arc;

use crate::{
    partition::{
        command::Command, handle::PartitionHandle, state::PartitionState, writer::PartitionWriter,
    },
    record::Record,
    segment::metadata::RecordLocation,
};

use arc_swap::ArcSwap;
use tokio::sync::{mpsc::channel, oneshot};

pub mod command;
pub mod handle;
pub mod state;
pub mod writer;

const DEFAULT_SEGMENT_BYTES: usize = 1 << 20;

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

    async fn append(&self, record: Record) {
        let (tx, rx) = oneshot::channel();
        self.handle
            .send(Command::Append { record, done: tx })
            .await
            .unwrap();
        rx.await.unwrap();
    }

    fn find_pos(&self, offset: u64) -> Option<RecordLocation> {
        self.handle.find(offset)
    }

    fn new(topic_id: String, id: u32, base_offset: u64) -> Self {
        let base_dir = format!("{}-{}", topic_id, id);

        let (tx, rx) = channel(1_000);
        let state = Arc::new(ArcSwap::from_pointee(PartitionState::new(base_offset)));
        let mut writer =
            PartitionWriter::new(rx, base_dir, DEFAULT_SEGMENT_BYTES, state.clone()).unwrap();
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
