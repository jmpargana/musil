use std::{collections::HashMap, sync::Arc, time::Instant};

use arc_swap::ArcSwap;
use tokio::sync::mpsc;

use network::protocol::{
    error_codes::ErrorCode,
    metadata::{CreateTopicResponse, TopicResponse},
    produce::acks::Acks,
};
use proto::{record::Record, record_batch::RecordBatch};
use storage::partition::{config::PartitionConfigBuilder, handle::PartitionHandle};

use crate::broker::{
    command::MetadataCommand,
    metadata_record::{MetadataRecord, PartitionRecord, TopicRecord},
    state::MetadataImage,
};

const INITIAL_EPOCH: i32 = 0;

pub struct MetadataActor {
    rx: mpsc::Receiver<MetadataCommand>,
    snapshot: Arc<ArcSwap<MetadataImage>>,
    path: String,
    handle: Arc<PartitionHandle>,
}

fn metadata_batch(metadata_record: MetadataRecord, epoch: i32) -> RecordBatch {
    let mut batch: RecordBatch = vec![Record::new(0, b"", &metadata_record.encode())].into();
    batch.partition_leader_epoch = epoch;
    batch
}

impl MetadataActor {
    pub fn new(rx: mpsc::Receiver<MetadataCommand>, path: String) -> Self {
        let config = PartitionConfigBuilder::default()
            .base_dir(path.clone())
            .partition_id(0)
            .topic_id("__cluster-metadata".to_string())
            .channel_size(100)
            .broker_id(0)
            .build()
            .unwrap();
        let handle = PartitionHandle::spawn(0, config);

        let segment_batches = {
            let state = handle.state.load_full();
            state
                .segments
                .first()
                .map(|s| s.fetch_all())
                .unwrap_or_default()
        };

        let mut topics: HashMap<String, (TopicRecord, Vec<PartitionRecord>)> = HashMap::new();

        for mut batch in segment_batches {
            let Ok(record) = Record::decode(&mut batch.records) else {
                continue;
            };
            let Some(metadata_record) = MetadataRecord::decode(&record.value) else {
                continue;
            };
            match metadata_record {
                MetadataRecord::Topic(t) => {
                    topics
                        .entry(t.name.clone())
                        .or_insert_with(|| (t, Vec::new()));
                }
                MetadataRecord::Partition(p) => {
                    if let Some(entry) = topics.get_mut(&p.topic_id) {
                        entry.1.push(p);
                    }
                }
            }
        }

        let mut snapshot = MetadataImage::empty();
        for (_, (topic, partitions)) in topics {
            snapshot = snapshot.create_topic(&path, &topic, &partitions);
        }

        Self {
            rx,
            handle,
            path,
            snapshot: Arc::new(ArcSwap::from_pointee(snapshot)),
        }
    }

    pub fn snapshot(&self) -> Arc<ArcSwap<MetadataImage>> {
        self.snapshot.clone()
    }

    pub async fn run(&mut self) {
        while let Some(c) = self.rx.recv().await {
            match c {
                MetadataCommand::CreateTopic { req, done } => {
                    let now = Instant::now();
                    let mut topic_record_refs: Vec<(TopicRecord, Vec<PartitionRecord>)> =
                        Vec::new();

                    let mut topic_responses = Vec::new();
                    for t in req.topics {
                        let topic = TopicRecord {
                            name: t.name.clone(),
                        };

                        let partitions: Vec<PartitionRecord> = (0..t.num_partitions)
                            .map(|p| PartitionRecord {
                                topic_id: t.name.clone(),
                                partition_id: p as u16,
                                replicas: vec![],
                                leader: 0,
                            })
                            .collect();

                        self.handle
                            .append(
                                metadata_batch(
                                    MetadataRecord::Topic(TopicRecord {
                                        name: t.name.clone(),
                                    }),
                                    INITIAL_EPOCH,
                                ),
                                Acks::None,
                            )
                            .await;

                        for p in &partitions {
                            self.handle
                                .append(
                                    metadata_batch(
                                        MetadataRecord::Partition(PartitionRecord {
                                            topic_id: p.topic_id.clone(),
                                            partition_id: p.partition_id,
                                            replicas: p.replicas.clone(),
                                            leader: p.leader,
                                        }),
                                        INITIAL_EPOCH,
                                    ),
                                    Acks::None,
                                )
                                .await;
                        }

                        topic_record_refs.push((topic, partitions));

                        topic_responses.push(TopicResponse {
                            name: t.name.clone(),
                            error_code: ErrorCode::None,
                            error_message: "".to_string(),
                            num_partitions: t.num_partitions,
                            replication_factor: t.replication_factor,
                        });
                    }

                    let current = self.snapshot.load_full();
                    let mut next = (*current).clone();
                    for (topic, partitions) in &topic_record_refs {
                        next = next.create_topic(&self.path, topic, partitions);
                    }
                    self.snapshot.store(Arc::new(next));

                    done.send(CreateTopicResponse {
                        throttle_time_ms: now.elapsed().as_millis() as u32,
                        topics: topic_responses,
                    })
                    .unwrap();
                }
                MetadataCommand::AddPartition {} => todo!(),
                MetadataCommand::RegisterBroker {} => todo!(),
            }
        }
    }
}
