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

    // TODO: actually this data should be inside the partition, because it's meant to be rolled as we append new stuff
    log_end_offset: u64,
    high_watermark: u64,

    // TODO: split between PartitionState which has SegmentMetadata and mutable segments
    snapshot: Arc<ArcSwap<PartitionState>>,
    // replication
    replication_tx: mpsc::Sender<Command>,
}

impl PartitionActor {
    pub fn new(
        rx: mpsc::Receiver<Command>,
        base_dir: String,
        segment_bytes: usize,
        snapshot: Arc<ArcSwap<PartitionState>>,
        replication_tx: mpsc::Sender<Command>,
    ) -> io::Result<Self> {
        let log_end_offset = 1;
        let high_watermark = 0;

        let cloned = base_dir.clone();

        let cfg = SegmentConfigBuilder::default()
            .base_dir(cloned.clone())
            .base_offset(log_end_offset)
            .segment_bytes(segment_bytes)
            .build()
            .unwrap();

        let mut active = ActiveSegment::new(cfg)?;
        let state = snapshot.load_full();

        let mut segments = state.segments.as_ref().to_vec();
        segments.push(active.publish());

        let next = Arc::new((*state).clone().consume(
            segments.into(),
            log_end_offset,
            high_watermark,
        ));
        snapshot.store(next);

        Ok(Self {
            rx,
            base_dir,
            active,
            segment_bytes,
            log_end_offset,
            high_watermark,
            snapshot,
            replication_tx,
        })
    }

    pub async fn run(&mut self) {
        while let Some(c) = self.rx.recv().await {
            match c {
                Command::Append { mut record, done } => {
                    record.add_offset(self.log_end_offset);
                    // TODO: handle error
                    self.active.append(record.clone()).unwrap();

                    self.log_end_offset += 1;

                    let current_active = self.active.publish();
                    let state = self.snapshot.load_full();

                    let mut segments = state.segments.as_ref().to_vec();
                    *segments.last_mut().unwrap() = current_active;

                    if self.active.size >= self.segment_bytes {
                        let cfg = SegmentConfigBuilder::default()
                            .base_dir(self.base_dir.to_string())
                            .base_offset(self.log_end_offset)
                            .build()
                            .unwrap();

                        let mut new_active = ActiveSegment::new(cfg).unwrap();

                        segments.push(new_active.publish());

                        self.active = new_active;
                    }

                    for replica in state.replicas.iter() {
                        let replica = replica.clone();
                        self.replication_tx
                            .send(Command::ReplicaRequest {
                                broker_id: replica.broker_id,
                                record: record.clone(),
                            })
                            .await
                            .unwrap();
                    }

                    if state.replicas.is_empty() {
                        self.high_watermark += 1;
                    }

                    // TODO: handle replication for acks=all

                    let next = Arc::new(PartitionState {
                        segments: segments.into(),
                        high_watermark: self.high_watermark,
                        log_end_offset: self.log_end_offset,
                        replicas: state.replicas.clone(),
                    });

                    self.snapshot.store(next);

                    done.send(()).unwrap();
                }
                Command::Shutdown => {
                    break;
                }
                // TODO: this needs to be tested
                Command::ReplicaAck { broker_id, offset } => {
                    let state = self.snapshot.load_full();
                    let next = (*state).clone().ack_replica(broker_id, offset);
                    let next = Arc::new(next);
                    self.snapshot.store(next);
                }
                // FIXME: refactor, this should never be used here
                Command::ReplicaRequest { broker_id, record } => todo!(),
            }
        }
    }
}
