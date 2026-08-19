use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use tokio::{
    sync::{
        mpsc::{self, channel, error::SendError},
        oneshot,
    },
    task::JoinHandle,
};

use crate::partition::{
    actor::{PartitionActor, PartitionActorConfig},
    command::PartitionCommand,
    config::PartitionConfig,
    state::PartitionState,
};
use proto::{
    fetch::{
        request::fetch_partition::FetchPartition,
        response::partition_response::PartitionResponse,
    },
    produce::{acks::Acks, response::partition_response::ProducePartitionResponse},
    record_batch::RecordBatch,
};

pub struct PartitionHandle {
    id: u32,
    tx: mpsc::Sender<PartitionCommand>,
    pub state: Arc<ArcSwap<PartitionState>>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl PartitionHandle {
    pub fn spawn(id: u32, config: PartitionConfig) -> Arc<Self> {
        let replicas = config.replicas.clone();
        let ch_size = config.channel_size;
        let partition_actor_config = PartitionActorConfig::from(config);

        let (tx, rx) = channel(ch_size);
        let state = Arc::new(ArcSwap::from_pointee(PartitionState::new(replicas)));
        let mut actor = PartitionActor::new(rx, state.clone(), partition_actor_config).unwrap();
        let join = tokio::spawn(async move {
            actor.run().await;
        });

        Arc::new(Self {
            id,
            tx,
            state,
            join: Mutex::new(Some(join)),
        })
    }

    pub async fn send(&self, c: PartitionCommand) -> Result<(), SendError<PartitionCommand>> {
        self.tx.send(c).await
    }

    pub async fn append(&self, record: RecordBatch, acks: Acks) -> ProducePartitionResponse {
        let (tx, rx) = oneshot::channel::<ProducePartitionResponse>();
        self.send(PartitionCommand::Append { record, acks, done: tx }).await.unwrap();
        rx.await.unwrap()
    }

    pub async fn fetch(&self, fetch_req: FetchPartition, replica_id: i32) -> PartitionResponse {
        let res = self.state.load_full().fetch(self.id, fetch_req);

        if replica_id >= 0 {
            self.send(PartitionCommand::UpdateReplicaLeo {
                replica_id: replica_id as u32,
                leo: res.log_start_offset,
            })
            .await
            .unwrap();
        }

        res
    }

    pub async fn shutdown(&self) {
        self.send(PartitionCommand::Shutdown).await.unwrap();
        if let Some(join) = self.join.lock().unwrap().take() {
            join.await.unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::partition::config::PartitionConfigBuilder;
    use proto::fetch::request::fetch_partition::FetchPartition;
    use proto::record::Record;
    use proto::record_batch::RecordBatch;

    use super::*;

    fn make_handle(dir: &tempdir::TempDir, segment_bytes: usize) -> Arc<PartitionHandle> {
        let cfg = PartitionConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .topic_id("test-topic".to_string())
            .partition_id(0)
            .broker_id(1)
            .segment_bytes(segment_bytes)
            .build()
            .unwrap();
        PartitionHandle::spawn(0, cfg)
    }

    fn record_batch(base_offset: u64, records: &[(&[u8], &[u8])]) -> RecordBatch {
        let mut encoded = Vec::new();
        for (i, (key, val)) in records.iter().enumerate() {
            encoded.extend(Record::new(i as u64, key, val).encode());
        }
        let records_data = Bytes::from(encoded);
        let batch_length = 49 + records_data.len() as u32;
        RecordBatch::from_compact(base_offset, batch_length, records.len() as u32, records_data)
    }

    fn fetch_req(offset: u64, max_bytes: u32) -> FetchPartition {
        FetchPartition { partition: 0, fetch_offset: offset, partition_max_bytes: max_bytes, high_watermark: 0 }
    }

    #[tokio::test]
    async fn append_then_fetch_returns_written_records() {
        let dir = tempdir::TempDir::new("handle-e2e").unwrap();
        let handle = make_handle(&dir, 1 << 20);

        let batch = record_batch(0, &[(b"key1", b"value1"), (b"key2", b"value2")]);
        let append_resp = handle.append(batch, Acks::Leader).await;

        assert_eq!(append_resp.base_offset, 0);
        assert_eq!(append_resp.index, 0);

        let state = handle.state.load_full();
        assert_eq!(state.log_end_offset, 2);
        assert_eq!(state.high_watermark, 2);

        let fetch_resp = handle.fetch(fetch_req(0, 1 << 20), -1).await;
        assert!(!fetch_resp.records.is_empty(), "fetch must return at least one batch");
        let total_records: u32 = fetch_resp.records.iter().map(|b| b.records_count).sum();
        assert_eq!(total_records, 2, "fetched record count must match appended");

        let fetched_batch = &fetch_resp.records[0];
        let (r0, consumed) = Record::decode_raw(&fetched_batch.records).unwrap();
        assert_eq!(r0.key, b"key1");
        assert_eq!(r0.value, b"value1");

        let (r1, _) = Record::decode_raw(&fetched_batch.records[consumed..]).unwrap();
        assert_eq!(r1.key, b"key2");
        assert_eq!(r1.value, b"value2");

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn append_multiple_batches_fetch_from_second() {
        let dir = tempdir::TempDir::new("handle-e2e").unwrap();
        let handle = make_handle(&dir, 1 << 20);

        let b0 = record_batch(0, &[(b"a", b"1")]);
        let b1 = record_batch(1, &[(b"b", b"2")]);
        handle.append(b0, Acks::Leader).await;
        handle.append(b1, Acks::Leader).await;

        let state = handle.state.load_full();
        assert_eq!(state.log_end_offset, 2);

        let fetch_resp = handle.fetch(fetch_req(1, 1 << 20), -1).await;
        assert!(!fetch_resp.records.is_empty());

        let first = &fetch_resp.records[0];
        let (r, _) = Record::decode_raw(&first.records).unwrap();
        assert_eq!(r.key, b"b");
        assert_eq!(r.value, b"2");

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn segment_rotation_both_segments_fetchable() {
        let dir = tempdir::TempDir::new("handle-e2e").unwrap();
        let handle = make_handle(&dir, 1);

        let b0 = record_batch(0, &[(b"k0", b"v0")]);
        let b1 = record_batch(1, &[(b"k1", b"v1")]);
        handle.append(b0, Acks::Leader).await;
        handle.append(b1, Acks::Leader).await;

        let state = handle.state.load_full();
        assert!(state.segments.len() >= 2, "rotation must have occurred");

        let r0 = handle.fetch(fetch_req(0, 1 << 20), -1).await;
        assert!(!r0.records.is_empty());
        let (rec, _) = Record::decode_raw(&r0.records[0].records).unwrap();
        assert_eq!(rec.key, b"k0");

        let r1 = handle.fetch(fetch_req(1, 1 << 20), -1).await;
        assert!(!r1.records.is_empty());
        let (rec, _) = Record::decode_raw(&r1.records[0].records).unwrap();
        assert_eq!(rec.key, b"k1");

        handle.shutdown().await;
    }
}
