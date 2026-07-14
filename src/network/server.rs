use std::sync::Arc;

use bytes::BytesMut;
use tokio::net::TcpListener;

use crate::{broker::Broker, network::connection::Connection};

pub struct SocketServer {
    broker: Arc<Broker>,
}

impl SocketServer {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    pub async fn listen(&self) {
        self.listen_on("0.0.0.0:8088").await;
    }

    pub async fn listen_on(&self, addr: &str) {
        let ln = TcpListener::bind(addr).await.unwrap();
        self.accept_loop(ln).await;
    }

    pub async fn listen_on_listener(&self, ln: TcpListener) {
        self.accept_loop(ln).await;
    }

    async fn accept_loop(&self, ln: TcpListener) {
        loop {
            let (stream, _) = ln.accept().await.unwrap();
            let conn = Connection {
                stream,
                broker: self.broker.clone(),
                read_buf: BytesMut::new(),
            };
            tokio::spawn(async move {
                conn.handle().await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use bytes::Bytes;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use crate::{
        broker::Broker,
        network::server::SocketServer,
        partition::{config::PartitionConfigBuilder, handle::PartitionHandle},
        protocol::{
            Frame,
            body::FrameBody,
            header::{ApiKey, RequestHeaderBuilder},
            produce::{
                acks::Acks,
                request::{
                    produce_partition::ProducePartition,
                    produce_request::ProduceRequest,
                    produce_topic::ProduceTopic,
                },
            },
        },
        storage::{record::Record, record_batch::RecordBatch},
        topic::TopicPartition,
    };

    fn make_broker(topic: &str, partition_id: u16) -> Arc<Broker> {
        let dir = tempdir::TempDir::new("srv-test").unwrap();
        let cfg = PartitionConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .topic_id(topic.to_string())
            .partition_id(partition_id)
            .broker_id(1)
            .segment_bytes(1 << 20)
            .build()
            .unwrap();
        let handle = PartitionHandle::spawn(partition_id as u32, cfg);
        std::mem::forget(dir);
        let mut partitions = HashMap::new();
        partitions.insert(TopicPartition { topic_id: topic.to_string(), partition_id }, handle);
        Arc::new(Broker::with_partitions(partitions))
    }

    fn produce_frame(topic: &str, partition_id: u16, correlation_id: u32) -> Frame {
        let encoded = Record::new(0, b"key", b"val").encode();
        let batch = RecordBatch {
            base_offset: 0,
            batch_length: 4 + encoded.len() as u32,
            records_count: 1,
            records: Bytes::from(encoded),
        };
        Frame {
            size: 0,
            header: RequestHeaderBuilder::default()
                .api_key(ApiKey::Produce)
                .api_version(0)
                .correlation_id(correlation_id)
                .client_id(None)
                .build()
                .unwrap(),
            body: FrameBody::Produce(ProduceRequest {
                transactional_id: 0,
                acks: Acks::Leader,
                timeout: Duration::ZERO,
                topics: vec![ProduceTopic {
                    topic: topic.to_string(),
                    partitions: vec![ProducePartition { index: partition_id, records: batch }],
                }],
            }),
        }
    }

    async fn start_server(broker: Arc<Broker>) -> tokio::net::TcpStream {
        let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = ln.local_addr().unwrap();
        let server = SocketServer::new(broker);
        tokio::spawn(async move {
            server.listen_on_listener(ln).await;
        });
        tokio::net::TcpStream::connect(addr).await.unwrap()
    }

    async fn send_frame(stream: &mut tokio::net::TcpStream, frame: Frame) {
        stream.write_all(&frame.encode()).await.unwrap();
    }

    async fn recv_bytes(stream: &mut tokio::net::TcpStream) -> Bytes {
        let size = stream.read_u32().await.unwrap();
        let mut buf = vec![0u8; size as usize];
        stream.read_exact(&mut buf).await.unwrap();
        Bytes::from(buf)
    }

    #[tokio::test]
    async fn server_accepts_connection_and_responds() {
        let broker = make_broker("orders", 0);
        let mut client = start_server(broker).await;

        send_frame(&mut client, produce_frame("orders", 0, 1)).await;

        let body = recv_bytes(&mut client).await;
        assert!(!body.is_empty());
    }

    #[tokio::test]
    async fn server_preserves_correlation_id() {
        let broker = make_broker("orders", 0);
        let mut client = start_server(broker).await;

        send_frame(&mut client, produce_frame("orders", 0, 99)).await;
        let body = recv_bytes(&mut client).await;

        let correlation_id = u32::from_be_bytes(body[8..12].try_into().unwrap());
        assert_eq!(correlation_id, 99);
    }

    #[tokio::test]
    async fn server_handles_multiple_sequential_clients() {
        let broker = make_broker("orders", 0);
        let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = ln.local_addr().unwrap();
        let server = SocketServer::new(broker);
        tokio::spawn(async move {
            server.listen_on_listener(ln).await;
        });

        for corr_id in [10u32, 20, 30] {
            let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
            send_frame(&mut client, produce_frame("orders", 0, corr_id)).await;
            let body = recv_bytes(&mut client).await;
            let got = u32::from_be_bytes(body[8..12].try_into().unwrap());
            assert_eq!(got, corr_id);
        }
    }

    #[tokio::test]
    async fn server_handles_concurrent_clients() {
        let broker = make_broker("orders", 0);
        let ln = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = ln.local_addr().unwrap();
        let server = SocketServer::new(broker);
        tokio::spawn(async move {
            server.listen_on_listener(ln).await;
        });

        let handles: Vec<_> = (0u32..5)
            .map(|i| {
                tokio::spawn(async move {
                    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
                    client
                        .write_all(&produce_frame("orders", 0, i).encode())
                        .await
                        .unwrap();
                    let size = client.read_u32().await.unwrap();
                    let mut buf = vec![0u8; size as usize];
                    client.read_exact(&mut buf).await.unwrap();
                    let correlation_id = u32::from_be_bytes(buf[8..12].try_into().unwrap());
                    assert_eq!(correlation_id, i);
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }
    }
}
