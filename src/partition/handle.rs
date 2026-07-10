use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::mpsc::{self, error::SendError};

use crate::{
    partition::{command::PartitionCommand, state::PartitionState},
    protocol::fetch::{
        request::fetch_partition::FetchPartition,
        response::partition_response::PartitionResponse,
    },
    segment::metadata::RecordLocation,
};

#[derive(Clone)]
pub struct PartitionHandle {
    tx: mpsc::Sender<PartitionCommand>,
    pub state: Arc<ArcSwap<PartitionState>>,
}

// TODO: this struct is doing nothing. Partition needs to be migrated here.
impl PartitionHandle {
    pub(crate) fn new(
        tx: mpsc::Sender<PartitionCommand>,
        state: Arc<arc_swap::ArcSwapAny<Arc<PartitionState>>>,
    ) -> Self {
        Self { tx, state }
    }

    pub async fn send(&self, c: PartitionCommand) -> Result<(), SendError<PartitionCommand>> {
        self.tx.send(c).await
    }

    pub fn find(&self, offset: u64) -> Option<RecordLocation> {
        self.state.load_full().find_pos(offset)
    }

    pub fn fetch(&self, partition_index: u32, fetch_req: &FetchPartition) -> PartitionResponse {
        self.state.load_full().fetch(partition_index, fetch_req)
    }
}
