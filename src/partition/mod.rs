use std::sync::Arc;

use crate::{
    partition::{
        command::Command, handle::PartitionHandle, state::PartitionState, writer::PartitionWriter,
    },
    record::Record,
};

use arc_swap::ArcSwap;
use tokio::sync::{mpsc::channel, oneshot};

pub mod command;
pub mod handle;
pub mod state;
pub mod writer;

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

    // TODO: define what should be read
    // for now we can return physical position
    fn find_pos(&self) -> u64 {
        0
    }

    fn new(id: u32, base_offset: u64) -> Self {
        let (tx, rx) = channel(1_000);
        let state = Arc::new(ArcSwap::from_pointee(PartitionState::new(base_offset)));
        let mut writer = PartitionWriter::new(rx, state.clone());
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
