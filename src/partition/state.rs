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
    segments: Arc<Vec<Arc<Segment>>>,
    pub log_end_offset: u64,
    pub high_watermark: u64,
}

impl PartitionState {
    pub fn new(base_offset: u64) -> Self {
        Self {
            segments: Arc::new(vec![]),
            log_end_offset: base_offset,
            high_watermark: base_offset - 1,
        }
    }

    pub fn find_pos(&self, offset: u64) -> Option<RecordLocation> {
        let idx = self
            .segments
            .partition_point(|segment| segment.base_offset <= offset);
        self.segments[idx].find_pos(offset)
    }

    pub fn replace_active(
        &self,
        active: Arc<Segment>,
        log_end_offset: u64,
        high_watermark: u64,
    ) -> Arc<Self> {
        let mut segments = *self.segments.as_ref().to_vec();
        *segments.last_mut().unwrap() = active;
        Arc::new(Self {
            segments: segments.into(),
            log_end_offset,
            high_watermark,
        })
    }

    pub fn roll(
        &self,
        new_active: Arc<Segment>,
        log_end_offset: u64,
        high_watermark: u64,
    ) -> Arc<Self> {
        let mut segments = self.segments.as_ref().to_vec();
        segments.push(new_active);
        Arc::new(Self {
            segments: segments.into(),
            log_end_offset,
            high_watermark,
        })
    }
}
