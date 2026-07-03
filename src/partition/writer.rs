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

    // mutable data
    active_segment: ActiveSegment,
    log_end_offset: u64,
    high_watermark: u64,

    // TODO: split between PartitionState which has SegmentMetadata and mutable segments
    snapshot: Arc<ArcSwap<PartitionState>>,
}

impl PartitionWriter {
    pub fn new(
        rx: mpsc::Receiver<Command>,
        base_dir: String,
        snapshot: Arc<ArcSwap<PartitionState>>,
    ) -> io::Result<Self> {
        let log_end_offset = 1;
        let high_watermark = 0;

        let cloned = base_dir.clone();

        Ok(Self {
            rx,
            base_dir,
            active_segment: ActiveSegment::new(SegmentOptions::with_defaults(
                &cloned,
                log_end_offset,
            ))?,
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
                    self.active_segment.append(record).unwrap();
                    self.log_end_offset += 1;

                    if self.active_segment.is_full() {
                        let current = self.snapshot.load_full();
                        current.push(self.active_segment.get_immutable());
                        self.snapshot.store(current);
                        self.active_segment = ActiveSegment::new(SegmentOptions::with_defaults(
                            &self.base_dir,
                            self.log_end_offset,
                        ))
                        .unwrap();
                    }
                    done.send(()).unwrap();
                }
                Command::Shutdown => {
                    break;
                }
            }
        }
    }
}
