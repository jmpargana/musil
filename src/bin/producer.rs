use std::io::stdin;

use clap::Parser;
use producer::{Producer, ProducerConfigBuilder, PublishPayload};

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(short, long, value_delimiter = ',', num_args = 1.., default_value = "127.0.0.1:9092")]
    bootstrap_servers: Vec<String>,

    #[arg(short, long)]
    topic: String,

    #[arg(short, long)]
    key: Option<String>,

    #[arg(short, long)]
    value: Option<String>,

    #[arg(long, default_value_t = 5 * 1000)]
    ms_wait: u64,

    #[arg(long, default_value_t = 4096)]
    max_bytes: u32,
}

pub fn parse_input_line(line: &str) -> (Option<String>, String) {
    match line.splitn(2, ':').collect::<Vec<_>>().as_slice() {
        [key, value] => (Some(key.to_string()), value.to_string()),
        [value] => (None, value.to_string()),
        _ => (None, line.to_string()),
    }
}

pub async fn run_once(producer: &Producer, topic: String, key: Option<String>, value: String) {
    producer
        .publish_once(PublishPayload { topic, key, value })
        .await
        .unwrap();
}

pub async fn run_loop(producer: &Producer, topic: String) {
    for line in stdin().lines() {
        match line {
            Ok(input) => {
                let input = input.trim().to_string();
                if input.is_empty() {
                    continue;
                }
                let (key, value) = parse_input_line(&input);
                producer
                    .publish_async(PublishPayload {
                        topic: topic.clone(),
                        key,
                        value,
                    })
                    .await
                    .unwrap();
            }
            Err(e) => {
                eprintln!("Error reading stdin: {e}");
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let producer = Producer::new(
        ProducerConfigBuilder::default()
            .bootstrap_servers(args.bootstrap_servers)
            .ms_wait(args.ms_wait)
            .max_bytes(args.max_bytes)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();

    if let Some(value) = args.value {
        run_once(&producer, args.topic, args.key, value).await;
    } else {
        run_loop(&producer, args.topic).await;
    }

    producer.shutdown().await.unwrap();
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use producer::{Producer, ProducerConfigBuilder, PublishPayload};
    use storage::protocol::{
        Frame,
        body::FrameBody,
        error_codes::ErrorCode,
        header::ApiKey,
        metadata::{BrokerMetadata, MetadataResponse, PartitionMetadata, TopicMetadata},
        produce::response::{
            partition_response::ProducePartitionResponse, produce_response::ProduceResponse,
            topic_response::ProduceTopicResponse,
        },
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::timeout,
    };

    use super::parse_input_line;

    // -----------------------------------------------------------------------
    // parse_input_line
    // -----------------------------------------------------------------------

    #[test]
    fn parse_colon_splits_key_value() {
        let (k, v) = parse_input_line("mykey:myvalue");
        assert_eq!(k.as_deref(), Some("mykey"));
        assert_eq!(v, "myvalue");
    }

    #[test]
    fn parse_no_colon_gives_no_key() {
        let (k, v) = parse_input_line("justvalue");
        assert!(k.is_none());
        assert_eq!(v, "justvalue");
    }

    #[test]
    fn parse_multiple_colons_only_first_splits() {
        let (k, v) = parse_input_line("key:val:extra");
        assert_eq!(k.as_deref(), Some("key"));
        assert_eq!(v, "val:extra");
    }

    #[test]
    fn parse_empty_key_before_colon() {
        let (k, v) = parse_input_line(":value");
        assert_eq!(k.as_deref(), Some(""));
        assert_eq!(v, "value");
    }

    #[test]
    fn parse_empty_value_after_colon() {
        let (k, v) = parse_input_line("key:");
        assert_eq!(k.as_deref(), Some("key"));
        assert_eq!(v, "");
    }

    #[test]
    fn parse_empty_line() {
        let (k, v) = parse_input_line("");
        assert!(k.is_none());
        assert_eq!(v, "");
    }

    // -----------------------------------------------------------------------
    // Mock broker helpers
    // -----------------------------------------------------------------------

    fn encode_metadata_response(topic: &str, num_partitions: usize) -> bytes::Bytes {
        let partitions = (0..num_partitions)
            .map(|i| PartitionMetadata {
                error_code: ErrorCode::None,
                partition_index: i as i32,
                leader_id: 0,
                replica_nodes: 1,
                isr_nodes: 1,
                offline_replicas: 0,
            })
            .collect();
        let resp = MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![BrokerMetadata {
                node_id: 0,
                host: "localhost".into(),
                port: 9092,
            }],
            controller_id: 0,
            topics: vec![TopicMetadata {
                error_code: ErrorCode::None,
                name: topic.to_string(),
                partitions,
            }],
            error_code: ErrorCode::None,
        };
        Frame::new(ApiKey::Metadata, FrameBody::MetadataResponse(resp)).encode()
    }

    fn encode_produce_response(topic: &str) -> bytes::Bytes {
        let resp = ProduceResponse {
            throttle_time_ms: 0,
            responses: vec![ProduceTopicResponse {
                topic: topic.to_string(),
                partition_responses: vec![ProducePartitionResponse::new(0, 0, 0)],
            }],
        };
        Frame::new(ApiKey::Produce, FrameBody::ProduceResponse(resp)).encode()
    }

    /// Spawns a mock broker that handles the metadata handshake then responds
    /// to every produce request. Returns the bound address.
    async fn spawn_mock_broker(topic: &'static str) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();

            // First request from Producer::new is always a metadata request.
            let size = sock.read_u32().await.unwrap();
            let mut buf = vec![0u8; size as usize];
            sock.read_exact(&mut buf).await.unwrap();
            sock.write_all(&encode_metadata_response(topic, 4))
                .await
                .unwrap();

            // Subsequent requests are produce requests.
            loop {
                let size = match sock.read_u32().await {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let mut buf = vec![0u8; size as usize];
                if sock.read_exact(&mut buf).await.is_err() {
                    break;
                }
                if sock
                    .write_all(&encode_produce_response(topic))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        addr
    }

    async fn make_producer(addr: std::net::SocketAddr) -> Producer {
        Producer::new(
            ProducerConfigBuilder::default()
                .bootstrap_servers(vec![addr.to_string()])
                .ms_wait(60_000)
                .max_bytes(1_000_000)
                .build()
                .unwrap(),
        )
        .await
        .unwrap()
    }

    // -----------------------------------------------------------------------
    // run_once
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_once_publishes_and_shuts_down() {
        let addr = spawn_mock_broker("test-topic").await;
        let producer = make_producer(addr).await;

        timeout(Duration::from_secs(2), async {
            super::run_once(&producer, "test-topic".into(), Some("k".into()), "v".into()).await;
            producer.shutdown().await.unwrap();
        })
        .await
        .expect("run_once did not complete in time");
    }

    #[tokio::test]
    async fn run_once_with_no_key() {
        let addr = spawn_mock_broker("test-topic").await;
        let producer = make_producer(addr).await;

        timeout(Duration::from_secs(2), async {
            super::run_once(
                &producer,
                "test-topic".into(),
                None,
                "valuewithnokey".into(),
            )
            .await;
            producer.shutdown().await.unwrap();
        })
        .await
        .expect("run_once (no key) did not complete");
    }

    // -----------------------------------------------------------------------
    // run_loop (driven via publish_async directly to avoid stdin coupling)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn publish_async_multiple_messages_then_shutdown() {
        let addr = spawn_mock_broker("stream-topic").await;
        let producer = Producer::new(
            ProducerConfigBuilder::default()
                .bootstrap_servers(vec![addr.to_string()])
                .ms_wait(50) // short timer so flush happens quickly
                .max_bytes(1_000_000)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

        let lines = ["user1:hello", "user2:world", "nokey", "k:v1:v2"];
        for line in lines {
            let (key, value) = parse_input_line(line);
            producer
                .publish_async(PublishPayload {
                    topic: "stream-topic".into(),
                    key,
                    value,
                })
                .await
                .unwrap();
        }

        timeout(Duration::from_secs(2), producer.shutdown())
            .await
            .expect("shutdown timed out")
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_after_run_once_does_not_hang() {
        let addr = spawn_mock_broker("t").await;
        let producer = make_producer(addr).await;
        super::run_once(&producer, "t".into(), None, "x".into()).await;

        timeout(Duration::from_secs(2), producer.shutdown())
            .await
            .expect("shutdown hung")
            .unwrap();
    }
}
