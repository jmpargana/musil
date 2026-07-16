use std::{collections::HashMap, sync::Arc, time::Instant};

use arc_swap::ArcSwap;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::{
    broker::{
        command::MetadataCommand,
        metadata_record::{MetadataRecord, PartitionRecord, TopicRecord},
        state::MetadataImage,
    },
    partition::{config::PartitionConfigBuilder, handle::PartitionHandle},
    protocol::{
        error_codes::ErrorCode,
        metadata::{CreateTopicResponse, TopicResponse},
        produce::acks::Acks,
    },
    storage::{record::Record, record_batch::RecordBatch},
};

pub struct MetadataActor {
    rx: mpsc::Receiver<MetadataCommand>,
    snapshot: Arc<ArcSwap<MetadataImage>>,
    path: String,
    // partition handle can live here, because it's always loaded to memory and never read throughout runtime
    handle: Arc<PartitionHandle>,
}

// REFACTOR:
fn make_batch(record: Record) -> RecordBatch {
    let encoded = record.encode();
    RecordBatch {
        base_offset: 0,
        batch_length: 4 + encoded.len() as u32,
        records_count: 1,
        records: Bytes::from(encoded),
    }
}

fn metadata_batch(metadata_record: MetadataRecord, timestamp: u64) -> RecordBatch {
    make_batch(Record {
        offset_delta: 0,
        timestamp,
        key: vec![],
        value: metadata_record.encode(),
    })
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

        // Accumulate: topic_id -> (TopicRecord, Vec<PartitionRecord>)
        let mut topics: HashMap<String, (TopicRecord, Vec<PartitionRecord>)> = HashMap::new();

        for batch in segment_batches {
            let Ok(record) = Record::decode(&batch.records) else {
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
                // TODO: refactor this very ugly repetition code
                MetadataCommand::CreateTopic { req, done } => {
                    let now = Instant::now();
                    let mut topic_record_refs: Vec<(TopicRecord, Vec<PartitionRecord>)> =
                        Vec::new();

                    let mut topic_responses = Vec::new();
                    for t in req.topics {
                        let ts = now.elapsed().as_millis() as u64;

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
                                    ts,
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
                                        ts,
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
                        // TODO: maybe this way of consuming ownership is not ideal for memory management.
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
