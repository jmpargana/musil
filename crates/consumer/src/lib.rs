use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use dashmap::DashMap;
use derive_builder::Builder;
use network::{
    protocol::metadata::PartitionMetadata,
    requests::{RequestError, request_fetch, request_metadata},
};
use proto::{record::Record, record_batch::RecordBatch};
use tokio::{net::TcpStream, sync::mpsc, time};

#[derive(Debug)]
pub enum ConsumerError {
    ConnErr(std::io::Error),
    ReqErr(RequestError),
    TopicNotFound,
}

pub struct ConsumerRecord {
    topic: String,
    partition: u16,
    inner: Record,
}

// Alternatively, this could be a From<(topic, partition, batch)>
fn batch_into_consumer_records(
    topic: String,
    partition: u16,
    batch: RecordBatch,
) -> Vec<ConsumerRecord> {
    let records: Vec<Record> = batch.into();
    records
        .into_iter()
        .map(|r| ConsumerRecord {
            topic: topic.clone(),
            partition,
            inner: r,
        })
        .collect()
}

// TODO: take format flag on top of this, ex.
//  - verbose / json / raw / hex / text / etc.
impl fmt::Display for ConsumerRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}-{}:{}] {}",
            self.topic,
            self.partition,
            String::from_utf8_lossy(&self.inner.key),
            String::from_utf8_lossy(&self.inner.value)
        )
    }
}

#[derive(Builder, Debug)]
pub struct ConsumerConfig {
    #[builder(default = 60)]
    pub metadata_refresh_timer_s: u64,
    pub topic: String,
    pub partition: Option<u16>,

    // Not passed when partition also not passed. Defaults to `--from-beginning` and loads and observes
    // All partitions concurrently
    #[builder(default = 0)]
    pub base_offset: u64,
    pub addr: String,
}

pub struct Consumer {
    pub rx: mpsc::Receiver<ConsumerRecord>,
}

impl Consumer {
    pub async fn new(cfg: ConsumerConfig) -> Result<Self, ConsumerError> {
        let (tx, rx) = mpsc::channel(1024);
        let actor = Arc::new(ConsumerActor::new(cfg, tx).await?);

        // TODO: maybe this could be triggered in the actor constructor directly?
        let a1 = actor.clone();
        tokio::spawn(async move {
            a1.run_metadata_loop().await;
        });

        let a2 = actor.clone();
        tokio::spawn(async move {
            a2.run_offset_sync_loop().await;
        });

        Ok(Self { rx })
    }
}

// TODO: need to add leader_id, replicas, etc.
#[derive(Default)]
struct ConsumerPartitionState {
    high_watermark: u64,
    commit_offset: u64,
}

impl ConsumerPartitionState {
    pub fn from_offset(fetch_offset: u64) -> Self {
        Self {
            high_watermark: 0,
            commit_offset: fetch_offset,
        }
    }
}

type ConsumerPartitions = DashMap<u16, ConsumerPartitionState>;

// In future, metadata manager and fetch menaager should be split into different structs.
// Also, we'll need consumer group manager (once we have replication).
// Finally, for persistent consumers, we also need commit_offsets at broker level, so consumers restart from correct place.
struct ConsumerActor {
    partitions: ConsumerPartitions,
    // Mutex needed here so there's no conflict in stream
    stream: tokio::sync::Mutex<TcpStream>,
    tx: mpsc::Sender<ConsumerRecord>,
    cfg: ConsumerConfig,
}

impl ConsumerActor {
    async fn new(
        cfg: ConsumerConfig,
        tx: mpsc::Sender<ConsumerRecord>,
    ) -> Result<Self, ConsumerError> {
        let mut stream = TcpStream::connect(&cfg.addr)
            .await
            .map_err(ConsumerError::ConnErr)?;

        let partitions = DashMap::new();

        if cfg.partition.is_none() {
            let _partitions = do_request_metadata(&cfg.topic, &mut stream).await?;
            _partitions.iter().for_each(|p| {
                partitions.insert(p.partition_index as u16, Default::default());
            });
        } else {
            partitions.insert(
                cfg.partition.expect("impossible"),
                ConsumerPartitionState::from_offset(cfg.base_offset),
            );
        }
        Ok(Self {
            partitions,
            stream: tokio::sync::Mutex::new(stream),
            tx,
            cfg,
        })
    }

    async fn single_request_metadata(&self) {
        let mut stream = self.stream.lock().await;
        // not doing anything for now
        // FIXME: when something will be done, the request shouldn't override single partition if in config.
        // maybe the best approach is to check self.cfg and only update leader_id of single observed replica.
        let _ = do_request_metadata(&self.cfg.topic, &mut stream);
    }

    // Loop used to update 2 things:
    // 1. Number of partitions
    // 2. Leader for each partition
    //
    // Leader is needed because in kafka reads and writes are always performed on the leader.
    // Partitions should all be loaded, even if there's filtering happening to single partition (debugging).
    async fn run_metadata_loop(&self) {
        let mut ticker = time::interval(Duration::from_secs(self.cfg.metadata_refresh_timer_s));
        loop {
            ticker.tick().await;
            self.single_request_metadata().await;
        }
    }

    // This function needs some love.
    // I'm still trying to figure out how to continuously receive commits without polling.
    // If metadata doesn't return high watermark, there must be another ApiKey which does,
    // problem is it's still a form of polling...
    async fn run_offset_sync_loop(&self) {
        loop {
            let mut stream = self.stream.lock().await;
            let partitions = self
                .partitions
                .iter()
                .map(|p| (*p.key(), p.commit_offset))
                .collect();
            match request_fetch(&mut stream, self.cfg.topic.clone(), partitions).await {
                Ok(fetch_response) => {
                    let mut total_records_received = 0;
                    for t in &fetch_response.responses {
                        for p in &t.partitions {
                            let mut records_received = 0u64;
                            let mut state = self
                                .partitions
                                .get_mut(&(p.partition_index as u16))
                                // TODO: return Result? Probably not possible since this is a loop
                                .expect("this means metadata broker above");

                            if p.high_watermark > state.high_watermark {
                                state.high_watermark = p.high_watermark;
                            }

                            for b in p.records.clone() {
                                let records = batch_into_consumer_records(
                                    t.topic.clone(),
                                    p.partition_index as u16,
                                    b,
                                );
                                records_received += records.len() as u64;
                                for r in records {
                                    let _ = self.tx.send(r).await;
                                }
                            }

                            state.commit_offset += records_received;
                            total_records_received += records_received;
                        }
                    }

                    if total_records_received == 0 {
                        drop(stream);
                        // FIXME: this is way too offen
                        time::sleep(Duration::from_millis(1000)).await;
                    }
                }
                Err(_) => {
                    drop(stream);
                    time::sleep(Duration::from_millis(5000)).await;
                }
            }
        }
    }
}

async fn do_request_metadata(
    topic: &str,
    stream: &mut TcpStream,
) -> Result<Vec<PartitionMetadata>, ConsumerError> {
    request_metadata(stream)
        .await
        .map_err(|e| ConsumerError::ReqErr(e))
        .and_then(|metadata| {
            metadata
                .topics
                .into_iter()
                .find(|t| t.name == topic)
                .ok_or(ConsumerError::TopicNotFound)
                .and_then(|t| Ok(t.partitions))
        })
}
