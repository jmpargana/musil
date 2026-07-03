use std::io;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::mpsc;

use crate::partition::command::Command;
use crate::partition::state::PartitionState;
use crate::segment::active::ActiveSegment;
use crate::segment::options::SegmentOptions;

pub struct PartitionWriter {
    rx: mpsc::Receiver<Command>,
    base_dir: String,
    segment_bytes: usize,

    // mutable data
    active: ActiveSegment,

    // TODO: actually this data should be inside the partition, because it's meant to be rolled as we append new stuff
    log_end_offset: u64,
    high_watermark: u64,

    // TODO: split between PartitionState which has SegmentMetadata and mutable segments
    snapshot: Arc<ArcSwap<PartitionState>>,
}

impl PartitionWriter {
    pub fn new(
        rx: mpsc::Receiver<Command>,
        base_dir: String,
        segment_bytes: usize,
        snapshot: Arc<ArcSwap<PartitionState>>,
    ) -> io::Result<Self> {
        let log_end_offset = 1;
        let high_watermark = 0;

        let cloned = base_dir.clone();
        let mut active =
            ActiveSegment::new(SegmentOptions::with_defaults(&cloned, log_end_offset))?;
        let state = snapshot.load_full();

        let mut segments = state.segments.as_ref().to_vec();
        segments.push(active.publish());

        Ok(Self {
            rx,
            base_dir,
            active,
            segment_bytes,
            log_end_offset,
            high_watermark,
            snapshot,
        })
    }

    pub async fn run(&mut self) {
        while let Some(c) = self.rx.recv().await {
            match c {
                Command::Append { mut record, done } => {
                    record.add_offset(self.log_end_offset);
                    // TODO: handle error
                    self.active.append(record).unwrap();

                    self.log_end_offset += 1;

                    // TODO: handle high_watermark

                    let current_active = self.active.publish();
                    let state = self.snapshot.load_full();

                    let mut segments = state.segments.as_ref().to_vec();
                    *segments.last_mut().unwrap() = current_active;

                    if self.active.size >= self.segment_bytes {
                        let mut new_active = ActiveSegment::new(SegmentOptions::with_defaults(
                            &self.base_dir,
                            self.log_end_offset,
                        ))
                        .unwrap();

                        segments.push(new_active.publish());

                        self.active = new_active;
                    }

                    let next = Arc::new(PartitionState {
                        segments: segments.into(),
                        high_watermark: self.high_watermark,
                        log_end_offset: self.log_end_offset,
                    });

                    self.snapshot.store(next);

                    done.send(()).unwrap();
                }
                Command::Shutdown => {
                    break;
                }
            }
        }
    }
}
