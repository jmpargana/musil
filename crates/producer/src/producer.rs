use bytes::BytesMut;
use network::protocol::{
    Frame,
    body::FrameBody,
    header::ApiKey,
    metadata::{MetadataRequest, MetadataResponse},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
};
use tracing::{debug, info};

use crate::{
    actor::ProducerActor, command::ProducerCommand, config::ProducerConfig, error::ProducerError,
    payload::PublishPayload,
};

pub struct Producer {
    tx: mpsc::Sender<ProducerCommand>,
    handle: JoinHandle<()>,
}

impl Producer {
    pub async fn new(cfg: ProducerConfig) -> Result<Self, ProducerError> {
        // TODO: use multiple brokers if first fails
        let addr = cfg.bootstrap_servers.first().unwrap();

        debug!(message="Connecting to broker", %addr);

        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|_| ProducerError::ConnErr)?;

        let body = MetadataRequest {
            topics: vec![], // sending null topics because I want to produce to all
            allow_auto_topic_creation: true,
        };
        let metadata_request = Frame::new(ApiKey::Metadata, FrameBody::Metadata(body));

        stream
            .write_all(&metadata_request.encode())
            .await
            .map_err(ProducerError::IoErr)?;
        let response_size = stream.read_u32().await.unwrap();
        let mut buf = BytesMut::zeroed(response_size as usize);

        stream
            .read_exact(&mut buf)
            .await
            .map_err(ProducerError::IoErr)?;

        let metadata_response = Frame::decode_response(&buf.freeze(), response_size)
            .map_err(|_| ProducerError::ParseErr)?;

        let metadata: MetadataResponse = metadata_response
            .body
            .try_into()
            .map_err(|_| ProducerError::FormatErr)?;

        info!(message="Available topics", topics=?metadata.topics.iter().map(|t|t.name.clone()).collect::<Vec<String>>());

        let (tx, rx) = mpsc::channel(4096);

        let mut producer = ProducerActor {
            stream,
            metadata_image: metadata,
            config: cfg,
            rx,
        };

        let handle = tokio::spawn(async move {
            producer.batcher().await;
        });

        Ok(Self { tx, handle })
    }

    pub async fn publish_async(&self, payload: PublishPayload) -> Result<(), ProducerError> {
        self.tx
            .send(ProducerCommand::Async(payload))
            .await
            .map_err(|_| ProducerError::ClientErr)
    }

    pub async fn publish_once(&self, payload: PublishPayload) -> Result<(), ProducerError> {
        self.tx
            .send(ProducerCommand::Sync(payload))
            .await
            .map_err(|_| ProducerError::ClientErr)
    }

    pub async fn shutdown(self) -> Result<(), ProducerError> {
        self.tx
            .send(ProducerCommand::Shutdown)
            .await
            .map_err(|_| ProducerError::UnknownErr)?;
        self.handle.await.map_err(|_| ProducerError::ChanClosed)
    }
}
