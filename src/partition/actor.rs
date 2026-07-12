use std::collections::VecDeque;
use std::io;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::{mpsc, oneshot};

use crate::partition::command::PartitionCommand;
use crate::partition::state::PartitionState;
use crate::protocol::produce::acks::Acks;
use crate::protocol::produce::response::partition_response::ProducePartitionResponse;
use crate::segment::config::SegmentConfigBuilder;
use crate::segment::log_segment::LogSegment;

struct PendingResponse {
    hw: u64,
    base_offset: u64,
    done: oneshot::Sender<ProducePartitionResponse>,
}

pub struct PartitionActor {
    rx: mpsc::Receiver<PartitionCommand>,
    base_dir: String,
    partition_id: u16,
    broker_id: u16,
    segment_bytes: usize,

    // mutable data
    active: LogSegment,

    // immutable data
    snapshot: Arc<ArcSwap<PartitionState>>,

    // As of now, this only works with Acks::All. If we want to have any number between 2..all,
    // custom logic in update replica is gonna be needed.
    // probably an extra variable should cut it.
    acks_pending_replication: VecDeque<PendingResponse>,
}

impl PartitionActor {
    pub fn new(
        rx: mpsc::Receiver<PartitionCommand>,
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

        let mut active = LogSegment::new(cfg)?;
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
            acks_pending_replication: VecDeque::new(), // TODO: add capacity based on replica size
        })
    }

    pub async fn run(&mut self) {
        while let Some(c) = self.rx.recv().await {
            match c {
                PartitionCommand::Append {
                    ref mut record,
                    acks,
                    done,
                } => {
                    // Kind of a hacky solution. The problem is that execution must continue with early send,
                    // which introduces a move of the `done` value.
                    // Wrapping it in an `Option<done>` allows the compiler to trust ownership can be moved safely.
                    let mut done = Some(done);

                    let state = self.snapshot.load_full();
                    let mut leo = state.log_end_offset;
                    let base_offset = leo;

                    let partition_response = ProducePartitionResponse::new(
                        self.partition_id as u32,
                        base_offset,
                        self.broker_id as i32,
                    );

                    if matches!(acks, Acks::None) {
                        done.take()
                            .unwrap()
                            // might need the Option trick to bypass clone here.
                            .send(partition_response.clone())
                            .unwrap();
                    }

                    self.active.append_batch(record);
                    record.update_base_offset(leo);

                    leo += record.records_count as u64;

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

                        let mut new_active = LogSegment::new(cfg).unwrap();

                        segments.push(new_active.publish());

                        self.active = new_active;
                    }

                    let mut hw = state.high_watermark;
                    if state.replicas.is_empty() {
                        hw += 1;
                    }

                    let next = Arc::new(PartitionState {
                        segments: segments.into(),
                        high_watermark: hw,
                        log_end_offset: leo,
                        replicas: state.replicas.clone(),
                    });

                    self.snapshot.store(next);

                    match &acks {
                        Acks::None => unreachable!(), // if, then matched above
                        Acks::Leader => {
                            done.take().unwrap().send(partition_response).unwrap();
                        }
                        Acks::All => {
                            self.acks_pending_replication.push_back(PendingResponse {
                                hw: leo,
                                base_offset,
                                done: done.take().unwrap(),
                            });
                        }
                    }
                }
                PartitionCommand::Shutdown => {
                    break;
                }
                PartitionCommand::UpdateReplicaLeo { replica_id, leo } => {
                    let state = self.snapshot.load_full();
                    let next = (*state).clone().ack_replica(replica_id, leo);
                    while let Some(ack) = self.acks_pending_replication.pop_front() {
                        if ack.hw > next.high_watermark {
                            self.acks_pending_replication.push_front(ack);
                            break;
                        }

                        ack.done
                            .send(ProducePartitionResponse::new(
                                self.partition_id as u32,
                                ack.base_offset,
                                self.broker_id as i32,
                            ))
                            .unwrap();
                    }
                    self.snapshot.store(Arc::new(next));
                }
            }
        }
    }
}
