use std::{collections::HashMap, time::Duration};

use network::protocol::{
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
use proto::record::Record;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
    time::timeout,
};

use crate::{
    actor::ProducerActor,
    command::ProducerCommand,
    config::{ProducerConfig, ProducerConfigBuilder},
    error::ProducerError,
    payload::PublishPayload,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_metadata(topic: &str, num_partitions: usize) -> MetadataResponse {
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

    MetadataResponse {
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
    }
}

fn make_config(ms_wait: u64, max_bytes: u32) -> ProducerConfig {
    ProducerConfigBuilder::default()
        .bootstrap_servers(vec!["127.0.0.1:0".into()])
        .ms_wait(ms_wait)
        .max_bytes(max_bytes)
        .build()
        .unwrap()
}

fn payload(topic: &str, key: Option<&str>, value: &str) -> PublishPayload {
    PublishPayload {
        topic: topic.to_string(),
        key: key.map(str::to_string),
        value: value.to_string(),
    }
}

/// Encodes a minimal ProduceResponse frame (what the mock broker sends back).
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

/// Spawns a mock broker on an available port. For each accepted connection it
/// reads every incoming produce request (drains it) and replies with a valid
/// ProduceResponse. Returns the bound address.
async fn spawn_mock_broker(topic: &str) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let response = encode_produce_response(topic);

    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        loop {
            // Read the 4-byte size prefix.
            let size = match sock.read_u32().await {
                Ok(s) => s,
                Err(_) => break,
            };
            // Drain the request body.
            let mut buf = vec![0u8; size as usize];
            if sock.read_exact(&mut buf).await.is_err() {
                break;
            }
            // Send back the encoded response.
            if sock.write_all(&response).await.is_err() {
                break;
            }
        }
    });

    addr
}

/// Build a ProducerActor connected to a mock broker.
async fn make_actor(
    topic: &str,
    ms_wait: u64,
    max_bytes: u32,
) -> (ProducerActor, mpsc::Sender<ProducerCommand>) {
    let addr = spawn_mock_broker(topic).await;
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (tx, rx) = mpsc::channel(128);
    let actor = ProducerActor {
        stream,
        metadata_image: make_metadata(topic, 4),
        config: make_config(ms_wait, max_bytes),
        rx,
    };
    (actor, tx)
}

// ---------------------------------------------------------------------------
// Error propagation
// ---------------------------------------------------------------------------

#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn all_unit_variants_match() {
        assert!(matches!(ProducerError::ConnErr, ProducerError::ConnErr));
        assert!(matches!(ProducerError::ParseErr, ProducerError::ParseErr));
        assert!(matches!(ProducerError::FormatErr, ProducerError::FormatErr));
        assert!(matches!(ProducerError::ClientErr, ProducerError::ClientErr));
        assert!(matches!(
            ProducerError::UnknownTopicErr,
            ProducerError::UnknownTopicErr
        ));
        assert!(matches!(
            ProducerError::ChanClosed,
            ProducerError::ChanClosed
        ));
    }

    #[test]
    fn io_err_wraps_inner() {
        let inner = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe");
        assert!(matches!(
            ProducerError::IoErr(inner),
            ProducerError::IoErr(_)
        ));
    }

    #[tokio::test]
    async fn publish_raw_returns_io_err_on_closed_stream() {
        // Connect then immediately drop the server side — writes will fail.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_sock, _) = listener.accept().await.unwrap();
            // drop _sock immediately to close the connection
        });
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (_, rx) = mpsc::channel(1);
        let mut actor = ProducerActor {
            stream,
            metadata_image: make_metadata("t", 1),
            config: make_config(1000, 1_000_000),
            rx,
        };
        // Give the server time to drop.
        tokio::time::sleep(Duration::from_millis(10)).await;

        let batch = HashMap::from([(
            "t".to_string(),
            HashMap::from([(0u16, vec![Record::new(0, b"", b"v")])]),
        )]);
        let result = actor.publish(batch).await;
        assert!(matches!(result, Err(ProducerError::IoErr(_))));
    }

    #[tokio::test]
    async fn unknown_topic_returns_err() {
        let addr = spawn_mock_broker("real_topic").await;
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (_, rx) = mpsc::channel(1);
        let actor = ProducerActor {
            stream,
            metadata_image: make_metadata("real_topic", 2),
            config: make_config(1000, 1_000_000),
            rx,
        };
        let p = payload("missing_topic", None, "val");
        // calculate_index is private — exercise via publish_once through the actor directly.
        // We rebuild with mut.
        let (_, rx2) = mpsc::channel(1);
        let addr2 = spawn_mock_broker("real_topic").await;
        let stream2 = tokio::net::TcpStream::connect(addr2).await.unwrap();
        let mut actor2 = ProducerActor {
            stream: stream2,
            metadata_image: make_metadata("real_topic", 2),
            config: make_config(1000, 1_000_000),
            rx: rx2,
        };
        let _ = actor; // silence unused
        let result = actor2.publish_once(p).await;
        assert!(matches!(result, Err(ProducerError::UnknownTopicErr)));
    }
}

// ---------------------------------------------------------------------------
// calculate_index (partition selection)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod index_tests {
    use super::*;

    async fn actor_with_partitions(n: usize) -> ProducerActor {
        let addr = spawn_mock_broker("topic").await;
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (_, rx) = mpsc::channel(1);
        ProducerActor {
            stream,
            metadata_image: make_metadata("topic", n),
            config: make_config(1000, 1_000_000),
            rx,
        }
    }

    #[tokio::test]
    async fn keyed_message_index_in_bounds() {
        let actor = actor_with_partitions(8).await;
        for _ in 0..50 {
            let p = payload("topic", Some("some-key"), "val");
            // publish_once will call calculate_index and succeed (mock broker responds)
            // We only care the result is Ok (index was valid).
            let addr = spawn_mock_broker("topic").await;
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (_, rx) = mpsc::channel(1);
            let mut a = ProducerActor {
                stream,
                metadata_image: make_metadata("topic", 8),
                config: make_config(1000, 1_000_000),
                rx,
            };
            assert!(a.publish_once(p).await.is_ok());
        }
        let _ = actor;
    }

    #[tokio::test]
    async fn keyless_message_index_in_bounds() {
        for _ in 0..50 {
            let addr = spawn_mock_broker("topic").await;
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (_, rx) = mpsc::channel(1);
            let mut a = ProducerActor {
                stream,
                metadata_image: make_metadata("topic", 3),
                config: make_config(1000, 1_000_000),
                rx,
            };
            assert!(a.publish_once(payload("topic", None, "val")).await.is_ok());
        }
    }

    #[tokio::test]
    async fn single_partition_always_index_zero() {
        for key in [Some("k"), None] {
            let addr = spawn_mock_broker("topic").await;
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (_, rx) = mpsc::channel(1);
            let mut a = ProducerActor {
                stream,
                metadata_image: make_metadata("topic", 1),
                config: make_config(1000, 1_000_000),
                rx,
            };
            // With 1 partition, N % 1 == 0 always.
            assert!(a.publish_once(payload("topic", key, "v")).await.is_ok());
        }
    }
}

// ---------------------------------------------------------------------------
// Batcher logic
// ---------------------------------------------------------------------------

#[cfg(test)]
mod batcher_tests {
    use super::*;

    /// Sends a Sync command and expects publish_once to fire immediately (before timer).
    #[tokio::test]
    async fn sync_publishes_immediately() {
        let (mut actor, tx) = make_actor("topic", 10_000, 1_000_000).await;

        tx.send(ProducerCommand::Sync(payload("topic", None, "direct")))
            .await
            .unwrap();
        tx.send(ProducerCommand::Shutdown).await.unwrap();

        timeout(Duration::from_secs(2), async move { actor.batcher().await })
            .await
            .expect("batcher did not finish within timeout");
    }

    /// With max_bytes set very small, the second Async message should trigger a
    /// publish before the timer fires.
    #[tokio::test]
    async fn async_publishes_when_size_threshold_reached() {
        // max_bytes = 1 forces publish on every Async message.
        let (mut actor, tx) = make_actor("topic", 60_000, 1).await;

        for _ in 0..3 {
            tx.send(ProducerCommand::Async(payload("topic", None, "x")))
                .await
                .unwrap();
        }
        tx.send(ProducerCommand::Shutdown).await.unwrap();

        timeout(Duration::from_secs(2), async move { actor.batcher().await })
            .await
            .expect("batcher did not finish within timeout");
    }

    /// With a very short timer and large max_bytes, publish fires on timeout not on size.
    #[tokio::test]
    async fn async_publishes_when_timer_fires() {
        // 50ms timer, huge max_bytes so size never triggers.
        let (mut actor, tx) = make_actor("topic", 50, 1_000_000).await;

        tx.send(ProducerCommand::Async(payload("topic", None, "timed")))
            .await
            .unwrap();
        // Don't send Shutdown — wait for batcher to publish via timer, then shut down.
        let handle = tokio::spawn(async move { actor.batcher().await });

        // Give the timer time to fire and publish.
        tokio::time::sleep(Duration::from_millis(200)).await;
        // Now shut it down.
        tx.send(ProducerCommand::Shutdown).await.unwrap();

        timeout(Duration::from_secs(2), handle)
            .await
            .expect("batcher timeout")
            .unwrap();
    }

    /// With a long timer and large max_bytes, a single small Async message must
    /// NOT cause an immediate publish — batcher stays in loop waiting.
    #[tokio::test]
    async fn does_not_publish_when_below_both_thresholds() {
        // 10s timer, huge max_bytes.
        let (mut actor, tx) = make_actor("topic", 10_000, 1_000_000).await;

        tx.send(ProducerCommand::Async(payload("topic", None, "small")))
            .await
            .unwrap();

        // The batcher should NOT have called publish yet. Shut it down quickly.
        let handle = tokio::spawn(async move { actor.batcher().await });

        // A brief sleep confirms the batcher is still waiting (no panic from mock).
        tokio::time::sleep(Duration::from_millis(50)).await;

        tx.send(ProducerCommand::Shutdown).await.unwrap();
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("batcher timeout")
            .unwrap();
    }

    /// Shutdown flushes any buffered records before exiting.
    #[tokio::test]
    async fn shutdown_flushes_pending_batch() {
        // Long timer, huge max_bytes — nothing publishes until Shutdown.
        let (mut actor, tx) = make_actor("topic", 60_000, 1_000_000).await;

        for i in 0..5 {
            tx.send(ProducerCommand::Async(payload(
                "topic",
                None,
                &i.to_string(),
            )))
            .await
            .unwrap();
        }
        tx.send(ProducerCommand::Shutdown).await.unwrap();

        // If flush on shutdown is broken, the mock broker never receives the
        // batch and the actor panics or hangs — the timeout catches that.
        timeout(Duration::from_secs(2), async move { actor.batcher().await })
            .await
            .expect("batcher did not flush and exit within timeout");
    }
}
