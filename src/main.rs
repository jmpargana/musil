use std::io::stdin;
use std::sync::Arc;

use broker::{Broker, config::BrokerConfig, network::server::SocketServer};
use bytes::BytesMut;
use clap::{Parser, Subcommand};
use consumer::{Consumer, ConsumerConfigBuilder};
use network::protocol::{Frame, body::FrameBody, header::ApiKey, metadata::CreateTopicRequest};
use producer::{Producer, ProducerConfigBuilder, PublishPayload};
use serde::Deserialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Parser)]
#[command(name = "musil", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Server(ServerArgs),
    Seeder(SeederArgs),
    Producer(ProducerArgs),
    Consumer(ConsumerArgs),
}

// --- Server ---

#[derive(Debug, Deserialize)]
struct ControllerConfig {
    controller: BrokerConfig,
    #[serde(default)]
    brokers: Vec<BrokerConfig>,
}

#[derive(Debug, Parser)]
struct ServerArgs {
    #[arg(short, long, default_value = "./data")]
    path: String,

    #[arg(short, long, default_value = "server.toml")]
    config: String,
}

async fn run_server(args: ServerArgs) {
    let settings = config::Config::builder()
        .add_source(config::File::with_name(&args.config))
        .build()
        .unwrap();

    let cfg = settings.try_deserialize::<ControllerConfig>().unwrap();

    let broker = Broker::new(args.path, cfg.controller, cfg.brokers);
    let srv = SocketServer::new(Arc::new(broker));
    srv.listen().await;
}

// --- Seeder ---

#[derive(Debug, Parser)]
struct SeederArgs {
    #[arg(short, long, value_delimiter = ',', num_args = 1.., default_value = "127.0.0.1:9092")]
    bootstrap_servers: Vec<String>,

    #[arg(short, long, default_value = "seeder.toml")]
    file: String,
}

async fn run_seeder(args: SeederArgs) {
    let settings = config::Config::builder()
        .add_source(config::File::with_name(&args.file))
        .build()
        .unwrap();

    let create_topic_request = settings.try_deserialize::<CreateTopicRequest>().unwrap();

    let addr = args.bootstrap_servers.first().unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let frame = Frame::new(ApiKey::CreateTopics, FrameBody::Topic(create_topic_request));

    stream.write_all(&frame.encode()).await.unwrap();

    let response_size = stream.read_u32().await.unwrap();
    let mut buf = BytesMut::zeroed(response_size as usize);

    stream.read_exact(&mut buf).await.unwrap();

    let create_topic_response = Frame::decode_response(&buf.freeze(), response_size).unwrap();
    println!("Successfully created topic: {create_topic_response:#?}");
}

// --- Producer ---

#[derive(Debug, Parser)]
struct ProducerArgs {
    #[arg(short, long, value_delimiter = ',', num_args = 1.., default_value = "127.0.0.1:9092")]
    bootstrap_servers: Vec<String>,

    #[arg(short, long)]
    topic: String,

    #[arg(short, long)]
    key: Option<String>,

    #[arg(short, long)]
    value: Option<String>,

    #[arg(long, default_value_t = 5000)]
    ms_wait: u64,

    #[arg(long, default_value_t = 4096)]
    max_bytes: u32,
}

fn parse_input_line(line: &str) -> (Option<String>, String) {
    match line.splitn(2, ':').collect::<Vec<_>>().as_slice() {
        [key, value] => (Some(key.to_string()), value.to_string()),
        [value] => (None, value.to_string()),
        _ => (None, line.to_string()),
    }
}

async fn run_producer(args: ProducerArgs) {
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
        producer
            .publish_once(PublishPayload {
                topic: args.topic,
                key: args.key,
                value,
            })
            .await
            .unwrap();
    } else {
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
                            topic: args.topic.clone(),
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

    producer.shutdown().await.unwrap();
}

// --- Consumer ---

#[derive(Debug, Parser)]
struct ConsumerArgs {
    #[arg(short, long, value_delimiter = ',', num_args = 1.., default_value = "127.0.0.1:9092")]
    bootstrap_servers: Vec<String>,

    #[arg(short, long)]
    topic: String,

    #[arg(short, long)]
    partition: Option<u16>,

    #[arg(short, long)]
    offset: u64,

    #[arg(short, long, default_value = "4096")]
    max_bytes: Option<u32>,
}

async fn run_consumer(args: ConsumerArgs) {
    let addr = args.bootstrap_servers.first().unwrap();
    let cfg = ConsumerConfigBuilder::default()
        .addr(addr.to_string())
        .topic(args.topic)
        .partition(args.partition)
        .base_offset(args.offset)
        .build()
        .unwrap();

    let mut consumer = Consumer::new(cfg).await.unwrap_or_else(|e| {
        eprintln!("Failed to connect to broker: {e:?}");
        std::process::exit(1);
    });

    while let Some(record) = consumer.rx.recv().await {
        println!("{record}");
    }
}

// --- Main ---

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .try_init()
        .expect("failed to initialize tracing");

    let cli = Cli::parse();

    match cli.command {
        Commands::Server(args) => run_server(args).await,
        Commands::Seeder(args) => run_seeder(args).await,
        Commands::Producer(args) => run_producer(args).await,
        Commands::Consumer(args) => run_consumer(args).await,
    }
}
