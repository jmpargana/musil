use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::mpsc::{self, error::SendError};

use crate::{
    command::Command, partition::state::PartitionState, segment::metadata::RecordLocation,
};

#[derive(Clone)]
pub struct PartitionHandle {
    tx: mpsc::Sender<Command>,
    pub state: Arc<ArcSwap<PartitionState>>,
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

    pub fn find(&self, offset: u64) -> Option<RecordLocation> {
        self.state.load_full().find_pos(offset)
    }
}
