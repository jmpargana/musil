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
    segments: Vec<Arc<Segment>>,
    log_end_offset: u64,
    high_watermark: u64,
}

impl PartitionState {
    pub fn new(base_offset: u64) -> Self {
        Self {
            segments: vec![],
            log_end_offset: base_offset,
            high_watermark: base_offset - 1,
        }
    }

    pub fn push(&mut self, seg: Arc<Segment>) {
        self.segments.push(seg);
    }

    pub fn find_pos(&self, offset: u64) -> Option<RecordLocation> {
        let idx = self
            .segments
            .partition_point(|segment| segment.base_offset <= offset);
        self.segments[idx].find_pos(offset)
    }
}
