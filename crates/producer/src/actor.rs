use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use bytes::BytesMut;
use murmur2::murmur2;
use network::protocol::{
    Frame, metadata::MetadataResponse, produce::request::produce_request::ProduceRequest,
};
use proto::record::Record;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    time,
};

use crate::{
    command::ProducerCommand, config::ProducerConfig, error::ProducerError, payload::PublishPayload,
};

pub struct ProducerActor {
    pub stream: TcpStream,
    pub metadata_image: MetadataResponse,
    pub config: ProducerConfig,
    // I'm not sure this is correct, but my idea is to have a single process running the loop.
    // Since this class can not be copied over, otherwise a Actor/Handle pair would be needed, this probably doesn't make any sense.
    pub rx: mpsc::Receiver<ProducerCommand>,
    // FIXME: take signal to terminate as well
}

impl ProducerActor {
    // This function runs in the background to wait for timer or full size of payload before publishing to broker.
    pub async fn batcher(&mut self) {
        let mut batch = HashMap::<String, HashMap<u16, Vec<Record>>>::new();

        // No timer running initially.
        let timer = time::sleep(Duration::from_secs(86400));
        tokio::pin!(timer);

        let mut timer_active = false;

        loop {
            tokio::select! {
                Some(item) = self.rx.recv() => {
                    match item {
                        ProducerCommand::Sync(publish_payload) => {
                            self.publish_once(publish_payload).await.unwrap();
                        },
                        ProducerCommand::Async(publish_payload) => {
                            let item = publish_payload;
                            let idx = self.calculate_index(&item).unwrap(); // TODO: fix error handling here
                            let partition = batch
                                .entry(item.topic.clone())
                                .or_default()
                                .entry(idx)
                                .or_default();

                            partition.push(item.into());

                            if !timer_active {
                                timer.as_mut().reset(
                                    ((Instant::now() + Duration::from_millis(self.config.ms_wait).into())).into()
                                );
                                timer_active = true;
                            }

                            // FIXME: This is unperformant, but I couldn't figure out a way to cleanly check size before.
                            let produce_request: ProduceRequest = batch.clone().into(); // because we don't pass acks, I could create a into_request() which takes that extra argument.
                            let produce_request: Frame = produce_request.into();

                            if produce_request.size >= self.config.max_bytes {
                                self.publish_raw(produce_request).await.unwrap();
                                timer_active = false;
                                // TODO: a move like below could also be used maybe
                                batch = HashMap::<String, HashMap<u16, Vec<Record>>>::new();
                            }
                        },
                        ProducerCommand::Shutdown => {
                            if !batch.is_empty() {
                                let batch_to_publish = std::mem::take(&mut batch);
                                self.publish(batch_to_publish).await.unwrap();
                            }
                            return;
                        },
                    }
                }

                _ = &mut timer, if timer_active && batch.len() > 0 => {
                    let batch_to_publish = std::mem::take(&mut batch);
                    self.publish(batch_to_publish).await.unwrap();
                    timer_active = false;
                }
            }
        }
    }

    pub async fn publish(
        &mut self,
        records: HashMap<String, HashMap<u16, Vec<Record>>>,
    ) -> Result<(), ProducerError> {
        let produce_request: ProduceRequest = records.into(); // because we don't pass acks, I could create a into_request() which takes that extra argument.
        let produce_request: Frame = produce_request.into();
        self.publish_raw(produce_request).await
    }

    // TODO: this only works for single broker. Sending to leader replica will require a refactor and multiple produce requests.
    pub async fn publish_raw(&mut self, produce_request: Frame) -> Result<(), ProducerError> {
        self.stream
            .write_all(&produce_request.encode())
            .await
            .map_err(|e| ProducerError::IoErr(e))?;

        let response_size = self
            .stream
            .read_u32()
            .await
            .map_err(|e| ProducerError::IoErr(e))?;
        let mut buf = BytesMut::zeroed(response_size as usize);

        self.stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| ProducerError::IoErr(e))?;

        let produce_response = Frame::decode_response(&buf.freeze(), response_size)
            .map_err(|_| ProducerError::ParseErr)?;
        println!("Successfully wrote: {produce_response:#?} into broker");
        Ok(())
    }

    pub async fn publish_once(&mut self, payload: PublishPayload) -> Result<(), ProducerError> {
        let idx = self.calculate_index(&payload)?;
        let topic = payload.topic.clone();
        self.publish(HashMap::from([(
            topic,
            HashMap::from([(idx, vec![payload.into()])]),
        )]))
        .await
        // TODO: should shutdown happen here or in caller?
    }

    // Calculates partition index for a given record. If key is provided, a kafka hash is used to find the partition,
    // otherwise round-robin.
    fn calculate_index(&self, payload: &PublishPayload) -> Result<u16, ProducerError> {
        let topic_metadata = self
            .metadata_image
            .topics
            .iter()
            .find(|t| t.name == payload.topic)
            .ok_or_else(|| ProducerError::UnknownTopicErr)?;

        let idx = match payload.key {
            Some(ref key) => {
                (murmur2(key.as_bytes(), rand::random()) as usize % topic_metadata.partitions.len())
                    as u16
            }
            None => rand::random_range(0..topic_metadata.partitions.len()) as u16,
        };
        Ok(idx)
    }
}
