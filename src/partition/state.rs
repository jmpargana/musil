use std::{mem, sync::Arc};

use derive_builder::Builder;
use tokio::sync::mpsc;

use crate::{
    record::Record,
    segment::{
        active::ActiveSegment,
        metadata::{RecordLocation, Segment},
    },
};

pub struct PartitionState {
    pub segments: Arc<Vec<Arc<Segment>>>,
    pub log_end_offset: u64,
    pub high_watermark: u64,
}

// TODO: partition should always start with 0
// bug: subtraction overflow
impl PartitionState {
    pub fn new() -> Self {
        Self {
            segments: Arc::new(vec![]),
            log_end_offset: 0,
            high_watermark: 0,
        }
    }

    pub fn find_pos(&self, offset: u64) -> Option<RecordLocation> {
        if offset >= self.high_watermark {
            return None;
        }
        let idx = self
            .segments
            .partition_point(|segment| segment.base_offset <= offset);
        // FIXME: something feels off about this
        self.segments[idx - 1].find_pos(offset)
    }
}
