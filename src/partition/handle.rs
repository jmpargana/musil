use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::mpsc::{self, error::SendError};

use crate::{
    partition::{command::Command, state::PartitionState},
    segment::metadata::RecordLocation,
};

#[derive(Clone)]
pub struct PartitionHandle {
    tx: mpsc::Sender<Command>,
    state: Arc<ArcSwap<PartitionState>>,
}
impl PartitionHandle {
    pub(crate) fn new(
        tx: mpsc::Sender<Command>,
        state: Arc<arc_swap::ArcSwapAny<Arc<PartitionState>>>,
    ) -> Self {
        Self { tx, state }
    }

    pub async fn send(&self, c: Command) -> Result<(), SendError<Command>> {
        self.tx.send(c).await
    }

    pub fn read(&self, offset: u64) -> Option<RecordLocation> {
        self.state.load_full().find_pos(offset)
    }
}
