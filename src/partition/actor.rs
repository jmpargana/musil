use std::io;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::mpsc;

use crate::command::Command;
use crate::partition::state::PartitionState;
use crate::segment::active::ActiveSegment;
use crate::segment::options::{SegmentConfig, SegmentConfigBuilder};

pub struct PartitionActor {
    rx: mpsc::Receiver<Command>,
    base_dir: String,
    segment_bytes: usize,

    // mutable data
    active: ActiveSegment,

    // immutable data
    snapshot: Arc<ArcSwap<PartitionState>>,
}

impl PartitionActor {
    pub fn new(
        rx: mpsc::Receiver<Command>,
        base_dir: String,
        segment_bytes: usize,
        snapshot: Arc<ArcSwap<PartitionState>>,
    ) -> io::Result<Self> {
        let cloned = base_dir.clone();

        let cfg = SegmentConfigBuilder::default()
            .base_dir(cloned.clone())
            .base_offset(0)
            .segment_bytes(segment_bytes)
            .build()
            .unwrap();

        let mut active = ActiveSegment::new(cfg)?;
        let state = snapshot.load_full();

        let mut segments = state.segments.as_ref().to_vec();
        segments.push(active.publish());

        let next = Arc::new((*state).clone().consume(segments.into(), 1, 0));
        snapshot.store(next);

        Ok(Self {
            rx,
            base_dir,
            active,
            segment_bytes,
            snapshot,
        })
    }

    pub async fn run(&mut self) {
        while let Some(c) = self.rx.recv().await {
            match c {
                Command::Append { mut record, done } => {
                    let state = self.snapshot.load_full();
                    let mut leo = state.log_end_offset;

                    record.add_offset(leo);

                    // TODO: handle error
                    self.active.append(record.clone()).unwrap();

                    leo += 1;

                    let current_active = self.active.publish();
                    let state = self.snapshot.load_full();

                    let mut segments = state.segments.as_ref().to_vec();
                    *segments.last_mut().unwrap() = current_active;

                    if self.active.size >= self.segment_bytes {
                        let cfg = SegmentConfigBuilder::default()
                            .base_dir(self.base_dir.to_string())
                            .base_offset(leo)
                            .build()
                            .unwrap();

                        let mut new_active = ActiveSegment::new(cfg).unwrap();

                        segments.push(new_active.publish());

                        self.active = new_active;
                    }

                    let mut hw = state.high_watermark;
                    if state.replicas.is_empty() {
                        hw += 1;
                    }

                    // TODO: handle replication for acks=all

                    let next = Arc::new(PartitionState {
                        segments: segments.into(),
                        high_watermark: hw,
                        log_end_offset: leo,
                        replicas: state.replicas.clone(),
                    });

                    self.snapshot.store(next);

                    done.send(()).unwrap();
                }
                Command::Shutdown => {
                    break;
                }
                // TODO: this needs to be tested
                Command::ReplicaAck {
                    broker_id,
                    offset,
                    done,
                } => {
                    let state = self.snapshot.load_full();
                    let next = (*state).clone().ack_replica(broker_id, offset);
                    let next = Arc::new(next);
                    self.snapshot.store(next);
                    done.send(()).unwrap();
                }
                // FIXME: refactor, this should never be used here
                Command::ReplicaRequest { broker_id, record } => todo!(),
                Command::AppendV2 { batch, done } => {
                    // TODO: implement
                    // self.active.append(record);

                    done.send(()).unwrap();
                }
                Command::Fetch {} => todo!(),
            }
        }
    }
}
