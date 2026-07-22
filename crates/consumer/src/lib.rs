use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use derive_builder::Builder;
use network::requests::{request_fetch, request_metadata};
use proto::{record::Record, record_batch::RecordBatch};
use tokio::{net::TcpStream, sync::mpsc, time};

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
    pub partition: u16,
    pub base_offset: u64,
    pub addr: String,
}

pub struct Consumer {
    pub rx: mpsc::Receiver<ConsumerRecord>,
}

impl Consumer {
    pub async fn new(cfg: ConsumerConfig) -> Self {
        let (tx, rx) = mpsc::channel(1024);
        let actor = Arc::new(ConsumerActor::new(cfg, tx).await);

        // TODO: maybe this could be triggered in the actor constructor directly?
        let a1 = actor.clone();
        tokio::spawn(async move {
            a1.run_metadata_loop().await;
        });

        let a2 = actor.clone();
        tokio::spawn(async move {
            a2.run_offset_sync_loop().await;
        });

        Self { rx }
    }
}

struct ConsumerActor {
    high_watermark: Arc<Mutex<u64>>,
    commit_offset: Arc<Mutex<u64>>,
    // Mutex needed here so there's no conflict in stream
    stream: tokio::sync::Mutex<TcpStream>,
    tx: mpsc::Sender<ConsumerRecord>,
    cfg: ConsumerConfig,
}

impl ConsumerActor {
    async fn new(cfg: ConsumerConfig, tx: mpsc::Sender<ConsumerRecord>) -> Self {
        let stream = TcpStream::connect(&cfg.addr).await.unwrap();
        let commit_offset = cfg.base_offset;
        Self {
            high_watermark: Arc::new(Mutex::new(0)),
            commit_offset: Arc::new(Mutex::new(commit_offset)),
            stream: tokio::sync::Mutex::new(stream),
            tx,
            cfg,
        }
    }

    async fn run_metadata_loop(&self) {
        let mut ticker = time::interval(Duration::from_secs(self.cfg.metadata_refresh_timer_s));
        loop {
            ticker.tick().await;
            let mut stream = self.stream.lock().await;
            if let Ok(metadata) = request_metadata(&mut stream).await {
                // This was my wrong assumption here. The metadata only adds new partitions but doesn't have high_watermark.
                for topic in &metadata.topics {
                    if topic.name == self.cfg.topic {
                        for partition in &topic.partitions {
                            if partition.partition_index == self.cfg.partition as i32 {
                                // TODO: update list of partitions when serving multiple ones.
                                let _ = partition;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn run_offset_sync_loop(&self) {
        loop {
            let commit = *self.commit_offset.lock().unwrap();

            let mut stream = self.stream.lock().await;
            match request_fetch(
                &mut stream,
                self.cfg.topic.clone(),
                self.cfg.partition as u32,
                commit,
            )
            .await
            {
                Ok(fetch_response) => {
                    let mut records_received = 0u64;
                    for t in &fetch_response.responses {
                        for p in &t.partitions {
                            // HW comes from fetch response, not metadata.
                            {
                                let mut hw = self.high_watermark.lock().unwrap();
                                if p.high_watermark > *hw {
                                    *hw = p.high_watermark;
                                }
                            }

                            for b in p.records.clone() {
                                let records = batch_into_consumer_records(
                                    t.topic.clone(),
                                    self.cfg.partition,
                                    b,
                                );
                                records_received += records.len() as u64;
                                for r in records {
                                    let _ = self.tx.send(r).await;
                                }
                            }
                        }
                    }

                    {
                        let mut co = self.commit_offset.lock().unwrap();
                        *co += records_received;
                    }

                    // Broker returned nothing — we're caught up, back off before polling again.
                    if records_received == 0 {
                        drop(stream);
                        time::sleep(Duration::from_millis(100)).await;
                    }
                }
                Err(_) => {
                    drop(stream);
                    time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }
}
