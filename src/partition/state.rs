use std::{mem, sync::Arc};

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

// bug: subtraction overflow
impl PartitionState {
    pub fn new(base_offset: u64) -> Self {
        Self {
            segments: Arc::new(vec![]),
            log_end_offset: base_offset,
            high_watermark: base_offset - 1,
        }
    }

    pub fn find_pos(&self, offset: u64) -> Option<RecordLocation> {
        if offset > self.high_watermark {
            return None;
        }
        let idx = self
            .segments
            .partition_point(|segment| segment.base_offset <= offset);
        // FIXME: something feels off about this
        self.segments[idx - 1].find_pos(offset)
    }
}
