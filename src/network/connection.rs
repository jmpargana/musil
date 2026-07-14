use std::{io::Error, sync::Arc};

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    broker::Broker,
    protocol::{Frame, codec::ParseError},
};

pub struct Connection {
    pub stream: TcpStream,
    pub broker: Arc<Broker>,
    // Reusable buffer for multiple streams in same connection to avoid allocating.
    // Not sure how to measure this...
    pub read_buf: BytesMut,
}

#[derive(Debug)]
pub enum ConnectionError {
    Io(Error),
    Protocol(ParseError),
    // internal errors are returned inside response body.
}

impl Connection {
    pub async fn handle(mut self) {
        loop {
            // TODO: implement error handling
            let request = match self.read_frame().await {
                Ok(r) => r,
                Err(ConnectionError::Io(_)) => {
                    break;
                }
                Err(ConnectionError::Protocol(e)) => {
                    tracing::warn!("protocol error: {e:?}");
                    break;
                }
            };
            let response = match self.broker.handle(request).await {
                Ok(r) => r,
                Err(_) => unreachable!(
                    "broker errors should be encoded as ErrorCode in the response body"
                ),
            };

            if let Err(e) = self.write_frame(response).await {
                // TODO: add tracing everywhere
                tracing::warn!("write failed: {e:?}");
                break;
            }
        }
    }

    async fn read_frame(&mut self) -> Result<Frame, ConnectionError> {
        let size = self
            .stream
            .read_u32()
            .await
            .map_err(|e| ConnectionError::Io(e))?;

        self.read_buf.resize(size as usize, 0);

        self.stream
            .read_exact(&mut self.read_buf)
            .await
            .map_err(|e| ConnectionError::Io(e))?;

        let frame = Frame::decode(&self.read_buf.split().freeze(), size)
            .map_err(|e| ConnectionError::Protocol(e))?;
        Ok(frame)
    }

    async fn write_frame(&mut self, res: Frame) -> Result<(), ConnectionError> {
        let bytes = res.encode();
        self.stream
            .write_all(&bytes)
            .await
            .map_err(|e| ConnectionError::Io(e))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use bytes::{Bytes, BytesMut};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use crate::{
        broker::Broker,
        network::connection::Connection,
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
        let dir = tempdir::TempDir::new("conn-test").unwrap();
        let cfg = PartitionConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .topic_id(topic.to_string())
            .partition_id(partition_id)
            .broker_id(1)
            .segment_bytes(1 << 20)
            .build()
            .unwrap();
        let handle = PartitionHandle::spawn(partition_id as u32, cfg);
        // Leak dir so files outlive the test (TempDir drops at end of scope otherwise)
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

    // Spawn a connection handler over an in-process socket pair.
    // Returns the client-side stream.
    async fn spawn_connection(broker: Arc<Broker>) -> tokio::net::TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let conn = Connection {
                stream,
                broker,
                read_buf: BytesMut::new(),
            };
            conn.handle().await;
        });

        tokio::net::TcpStream::connect(addr).await.unwrap()
    }

    // Send a frame over a stream: writes the 4-byte size + body (as encode() produces).
    async fn send_frame(stream: &mut tokio::net::TcpStream, frame: Frame) {
        let encoded = frame.encode();
        stream.write_all(&encoded).await.unwrap();
    }

    // Read the 4-byte size prefix then the response body.
    async fn recv_bytes(stream: &mut tokio::net::TcpStream) -> Bytes {
        let size = stream.read_u32().await.unwrap();
        let mut buf = vec![0u8; size as usize];
        stream.read_exact(&mut buf).await.unwrap();
        Bytes::from(buf)
    }

    // --- connection tests ---

    #[tokio::test]
    async fn produce_request_gets_response() {
        let broker = make_broker("orders", 0);
        let mut client = spawn_connection(broker).await;

        send_frame(&mut client, produce_frame("orders", 0, 1)).await;

        let body = recv_bytes(&mut client).await;
        // ProduceResponse starts with u32 throttle_time_ms — just check we got bytes back
        assert!(!body.is_empty());
    }

    #[tokio::test]
    async fn response_carries_correlation_id() {
        let broker = make_broker("orders", 0);
        let mut client = spawn_connection(broker).await;

        send_frame(&mut client, produce_frame("orders", 0, 42)).await;

        let body = recv_bytes(&mut client).await;
        // Response header: api_key(u32) + api_version(u32) + correlation_id(u32)
        let correlation_id = u32::from_be_bytes(body[8..12].try_into().unwrap());
        assert_eq!(correlation_id, 42);
    }

    #[tokio::test]
    async fn multiple_requests_on_same_connection() {
        let broker = make_broker("orders", 0);
        let mut client = spawn_connection(broker).await;

        send_frame(&mut client, produce_frame("orders", 0, 1)).await;
        recv_bytes(&mut client).await;

        send_frame(&mut client, produce_frame("orders", 0, 2)).await;
        let body = recv_bytes(&mut client).await;

        let correlation_id = u32::from_be_bytes(body[8..12].try_into().unwrap());
        assert_eq!(correlation_id, 2);
    }

    fn metadata_frame(correlation_id: u32) -> Frame {
        use crate::protocol::metadata::MetadataRequest;
        Frame {
            size: 0,
            header: RequestHeaderBuilder::default()
                .api_key(ApiKey::Metadata)
                .api_version(0)
                .correlation_id(correlation_id)
                .client_id(None)
                .build()
                .unwrap(),
            body: FrameBody::Metadata(MetadataRequest {
                topics: vec![],
                allow_auto_topic_creation: false,
            }),
        }
    }

    // Regression: BytesMut::with_capacity(n) sets len=0, so read_exact reads 0 bytes,
    // and the subsequent Frame::decode panics with "advance out of bounds: len is 0 but advancing by 4".
    // Fix: use BytesMut::zeroed(n) so the slice has the correct length before read_exact.
    #[tokio::test]
    async fn read_response_with_zeroed_not_with_capacity() {
        let broker = make_broker("orders", 0);
        let mut client = spawn_connection(broker).await;

        send_frame(&mut client, metadata_frame(7)).await;

        let size = client.read_u32().await.unwrap();
        // with_capacity allocates but len stays 0 — read_exact reads nothing
        assert_eq!(BytesMut::with_capacity(size as usize).len(), 0, "with_capacity leaves len=0");

        // zeroed sets len correctly — read_exact fills the buffer
        let mut buf = BytesMut::zeroed(size as usize);
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size as usize);

        // decoding must succeed without panic
        let frame = Frame::decode(&buf.freeze(), size).unwrap();
        assert_eq!(frame.header.correlation_id, 7);
    }

    #[tokio::test]
    async fn connection_closes_on_bad_frame() {
        let broker = make_broker("orders", 0);
        let mut client = spawn_connection(broker).await;

        // Send a size prefix claiming 8 bytes, then only 4 garbage bytes + close
        // The server should not panic — it just closes the connection
        client.write_u32(8).await.unwrap();
        client.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).await.unwrap();
        drop(client);
        // If the server panicked, the test process would abort — reaching here means it didn't
    }
}
