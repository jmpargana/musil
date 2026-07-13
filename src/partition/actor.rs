use std::collections::VecDeque;
use std::io;
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use derive_builder::Builder;
use tokio::sync::{mpsc, oneshot};

use crate::partition::command::PartitionCommand;
use crate::partition::config::PartitionConfig;
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

#[derive(Builder)]
pub struct PartitionActorConfig {
    pub base_dir: String,
    pub segment_bytes: usize,
    pub broker_id: u16,
    pub partition_id: u16,
}

impl From<PartitionConfig> for PartitionActorConfig {
    fn from(cfg: PartitionConfig) -> Self {
        let base_path = Path::new(&cfg.base_dir);
        let topic_partition_name = format!("{}-{}", cfg.topic_id, cfg.partition_id);
        let base_dir = base_path
            .join(topic_partition_name)
            .as_path()
            .to_str()
            .unwrap()
            .to_string();

        PartitionActorConfigBuilder::default()
            .broker_id(cfg.broker_id)
            .partition_id(cfg.partition_id)
            .segment_bytes(cfg.segment_bytes)
            .base_dir(base_dir)
            .build()
            .unwrap()
    }
}

impl PartitionActor {
    pub fn new(
        rx: mpsc::Receiver<PartitionCommand>,
        snapshot: Arc<ArcSwap<PartitionState>>,
        config: PartitionActorConfig,
    ) -> io::Result<Self> {
        let cloned = config.base_dir.clone();

        let cfg = SegmentConfigBuilder::default()
            .base_dir(cloned.clone())
            .base_offset(0)
            .segment_bytes(config.segment_bytes)
            .build()
            .unwrap();

        let mut active = LogSegment::new(cfg)?;
        let state = snapshot.load_full();

        let mut segments = state.segments.as_ref().to_vec();
        segments.push(active.publish());

        let next = Arc::new((*state).clone().consume(segments.into(), 0, 0));
        snapshot.store(next);

        Ok(Self {
            rx,
            active,
            snapshot,
            acks_pending_replication: VecDeque::new(), // TODO: add capacity based on replica size
            base_dir: config.base_dir,
            broker_id: config.broker_id,
            partition_id: config.partition_id,
            segment_bytes: config.segment_bytes,
        })
    }

    #[cfg(test)]
    pub fn snapshot(&self) -> Arc<ArcSwap<PartitionState>> {
        self.snapshot.clone()
    }

    pub async fn run(&mut self) {
        while let Some(c) = self.rx.recv().await {
            match c {
                PartitionCommand::Append {
                    mut record,
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

                    record.update_base_offset(leo);
                    self.active.append_batch(&record).unwrap();

                    leo += record.records_count as u64;

                    let current_active = self.active.publish();
                    let state = self.snapshot.load_full();

                    let mut segments = state.segments.as_ref().to_vec();
                    *segments.last_mut().unwrap() = current_active;

                    if self.active.size >= self.segment_bytes {
                        let cfg = SegmentConfigBuilder::default()
                            .base_dir(self.base_dir.to_string())
                            .base_offset(leo)
                            .segment_bytes(self.segment_bytes)
                            .build()
                            .unwrap();

                        let mut new_active = LogSegment::new(cfg).unwrap();

                        segments.push(new_active.publish());

                        self.active = new_active;
                    }

                    let mut hw = state.high_watermark;
                    if state.replicas.is_empty() {
                        hw += record.records_count as u64;
                    }

                    let next = Arc::new(PartitionState {
                        segments: segments.into(),
                        high_watermark: hw,
                        log_end_offset: leo,
                        replicas: state.replicas.clone(),
                    });

                    self.snapshot.store(next);

                    match &acks {
                        Acks::None | Acks::Leader => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::ReplicaMetadata;
    use crate::storage::record_batch::RecordBatch;
    use bytes::Bytes;
    use tempdir::TempDir;
    use tokio::sync::oneshot;

    fn make_dir() -> TempDir {
        TempDir::new("rafka-actor-test").unwrap()
    }

    fn make_batch(base_offset: u64, records_count: u32, payload: &[u8]) -> RecordBatch {
        let batch_length = 4 + payload.len() as u32;
        RecordBatch {
            base_offset,
            batch_length,
            records_count,
            records: Bytes::copy_from_slice(payload),
        }
    }

    fn spawn_actor(
        dir: &TempDir,
        replicas: Vec<ReplicaMetadata>,
        segment_bytes: usize,
    ) -> (
        mpsc::Sender<PartitionCommand>,
        Arc<ArcSwap<PartitionState>>,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::channel(64);
        let state = Arc::new(ArcSwap::from_pointee(PartitionState::new(replicas)));
        let cfg = PartitionActorConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .segment_bytes(segment_bytes)
            .broker_id(1)
            .partition_id(0)
            .build()
            .unwrap();
        let mut actor = PartitionActor::new(rx, state.clone(), cfg).unwrap();
        let handle = tokio::spawn(async move { actor.run().await });
        (tx, state, handle)
    }

    async fn append(
        tx: &mpsc::Sender<PartitionCommand>,
        batch: RecordBatch,
        acks: Acks,
    ) -> ProducePartitionResponse {
        let (done_tx, done_rx) = oneshot::channel();
        tx.send(PartitionCommand::Append {
            record: batch,
            acks,
            done: done_tx,
        })
        .await
        .unwrap();
        done_rx.await.unwrap()
    }

    #[tokio::test]
    async fn high_watermark_advances_by_records_count_not_one() {
        let dir = make_dir();
        let (tx, state, handle) = spawn_actor(&dir, vec![], 1 << 20);

        let batch = make_batch(0, 5, b"aaaaa");
        append(&tx, batch, Acks::Leader).await;

        let snap = state.load_full();
        // With the bug: hw == 1. Fixed: hw == 5.
        assert_eq!(
            snap.high_watermark, 5,
            "hw must equal records_count (5), not 1"
        );
        assert_eq!(snap.log_end_offset, 5);

        tx.send(PartitionCommand::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn leo_accumulates_across_multiple_appends() {
        let dir = make_dir();
        let (tx, state, handle) = spawn_actor(&dir, vec![], 1 << 20);

        append(&tx, make_batch(0, 3, b"aaa"), Acks::Leader).await;
        append(&tx, make_batch(3, 2, b"bb"), Acks::Leader).await;

        let snap = state.load_full();
        assert_eq!(snap.log_end_offset, 5);
        // With the bug hw==2 (1+1). Fixed: hw==5.
        assert_eq!(snap.high_watermark, 5);

        tx.send(PartitionCommand::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn acks_none_responds_immediately() {
        let dir = make_dir();
        let (tx, _state, handle) = spawn_actor(&dir, vec![], 1 << 20);

        let batch = make_batch(0, 1, b"x");
        let resp = append(&tx, batch, Acks::None).await;

        assert_eq!(resp.index, 0);
        assert_eq!(resp.base_offset, 0);

        tx.send(PartitionCommand::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn acks_leader_responds_with_correct_base_offset() {
        let dir = make_dir();
        let (tx, _state, handle) = spawn_actor(&dir, vec![], 1 << 20);

        let r1 = append(&tx, make_batch(0, 2, b"ab"), Acks::Leader).await;
        assert_eq!(r1.base_offset, 0);

        let r2 = append(&tx, make_batch(2, 3, b"cde"), Acks::Leader).await;
        assert_eq!(r2.base_offset, 2);

        tx.send(PartitionCommand::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn segment_rotates_and_state_has_two_segments() {
        let dir = make_dir();
        // segment_bytes=1 forces rotation after every batch (batch_length > 0)
        let (tx, state, handle) = spawn_actor(&dir, vec![], 1);

        append(&tx, make_batch(0, 1, b"x"), Acks::Leader).await;
        append(&tx, make_batch(1, 1, b"y"), Acks::Leader).await;

        let snap = state.load_full();
        assert!(
            snap.segments.len() >= 2,
            "expected >=2 segments after rotation, got {}",
            snap.segments.len()
        );

        tx.send(PartitionCommand::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn acks_all_waits_for_replica_ack() {
        let dir = make_dir();
        let replica = ReplicaMetadata::empty("broker-2".to_string(), 42);
        let (tx, state, handle) = spawn_actor(&dir, vec![replica], 1 << 20);

        let batch = make_batch(0, 1, b"z");
        let (done_tx, _done_rx) = oneshot::channel();
        tx.send(PartitionCommand::Append {
            record: batch,
            acks: Acks::All,
            done: done_tx,
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let snap = state.load_full();
        assert_eq!(
            snap.high_watermark, 0,
            "hw must not advance until replica acks"
        );

        tx.send(PartitionCommand::UpdateReplicaLeo {
            replica_id: 42,
            leo: 1,
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let snap = state.load_full();
        assert_eq!(snap.high_watermark, 1);

        tx.send(PartitionCommand::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn acks_all_flushes_in_fifo_order() {
        let dir = make_dir();
        let replica = ReplicaMetadata::empty("broker-2".to_string(), 99);
        let (tx, _state, handle) = spawn_actor(&dir, vec![replica], 1 << 20);

        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        tx.send(PartitionCommand::Append {
            record: make_batch(0, 1, b"a"),
            acks: Acks::All,
            done: tx1,
        })
        .await
        .unwrap();

        tx.send(PartitionCommand::Append {
            record: make_batch(1, 1, b"b"),
            acks: Acks::All,
            done: tx2,
        })
        .await
        .unwrap();

        tx.send(PartitionCommand::UpdateReplicaLeo {
            replica_id: 99,
            leo: 2,
        })
        .await
        .unwrap();

        let r1 = tokio::time::timeout(std::time::Duration::from_millis(200), rx1)
            .await
            .expect("r1 timed out")
            .unwrap();
        let r2 = tokio::time::timeout(std::time::Duration::from_millis(200), rx2)
            .await
            .expect("r2 timed out")
            .unwrap();

        assert_eq!(r1.base_offset, 0);
        assert_eq!(r2.base_offset, 1);

        tx.send(PartitionCommand::Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_completes() {
        let dir = make_dir();
        let (tx, _state, handle) = spawn_actor(&dir, vec![], 1 << 20);
        tx.send(PartitionCommand::Shutdown).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("actor did not shut down in time")
            .unwrap();
    }
}
