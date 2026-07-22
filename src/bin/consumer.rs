use bytes::BytesMut;
use clap::Parser;
use consumer::{Consumer, ConsumerConfig, ConsumerConfigBuilder};
use network::protocol::{
    Frame,
    body::FrameBody,
    fetch::request::{
        fetch_partition::FetchPartition, fetch_request::FetchRequest, fetch_topic::FetchTopic,
    },
    header::ApiKey,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    // TODO: directly parse comma-seperated values including host+port config.
    #[arg(short, long, value_delimiter = ',', num_args = 1.., default_value = "127.0.0.1:9092")]
    bootstrap_servers: Vec<String>,

    #[arg(short, long)]
    topic: String,

    #[arg(short, long)]
    partition: u16,

    #[arg(short, long)]
    offset: u64,

    #[arg(short, long, default_value = "4096")]
    max_bytes: Option<u32>,
}

// The correct way to do this is to wait to accumulate some records.
#[tokio::main]
async fn main() {
    let args = Args::parse();

    let addr = args.bootstrap_servers.first().unwrap();
    let cfg = ConsumerConfigBuilder::default()
        .addr(addr.to_string())
        .topic(args.topic)
        .partition(args.partition)
        .base_offset(args.offset)
        .build()
        .unwrap();
    let mut consumer = Consumer::new(cfg).await;

    while let Some(record) = consumer.rx.recv().await {
        println!("{record}");
    }
}
