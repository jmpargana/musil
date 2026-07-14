use std::{hash::Hash, net::IpAddr, time::Duration};

use bytes::BytesMut;
use clap::Parser;
use murmur2::murmur2;
use rafka::{
    protocol::{
        Frame,
        body::FrameBody,
        header::{ApiKey, RequestHeader},
        metadata::MetadataRequest,
        produce::{
            acks::Acks,
            request::{
                produce_partition::ProducePartition, produce_request::ProduceRequest,
                produce_topic::ProduceTopic,
            },
        },
    },
    storage::{record::Record, record_batch::RecordBatch},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
};

use rand::RngExt;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    // TODO: directly parse comma-seperated values including host+port config.
    #[arg(short, long, value_delimiter = ',', num_args = 1..)]
    bootstrap_servers: Vec<String>,

    #[arg(short, long)]
    topic: String,

    #[arg(short, long)]
    key: Option<String>,

    #[arg(short, long)]
    value: String,
}

// The correct way to do this is to wait to accumulate some records.
#[tokio::main]
async fn main() {
    let args = Args::parse();

    // 0. Establish connection

    let addr = args.bootstrap_servers.first().unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let body = MetadataRequest {
        topics: vec![],
        allow_auto_topic_creation: true,
    };
    let metadata_request = Frame::new(ApiKey::Metadata, FrameBody::Metadata(body));

    // 1. Call metadata against bootstrap server
    stream.write_all(&metadata_request.encode()).await.unwrap();
    let response_size = stream.read_u32().await.unwrap();
    // let mut buf = Vec::with_capacity(response_size as usize);
    let mut buf = BytesMut::with_capacity(response_size as usize);

    stream.read_exact(&mut buf).await.unwrap();

    let metadata_response = Frame::decode(&buf.freeze(), response_size).unwrap();

    // 2. Perform hash or pick random partition
    let metadata = if let FrameBody::MetadataResponse(metadata) = metadata_response.body {
        metadata
    } else {
        panic!("should only receive metadata response");
    };

    let topic_metadata = metadata
        .topics
        .iter()
        .find(|t| t.name == args.topic)
        .unwrap();

    let index = match args.key {
        Some(ref key) => {
            murmur2(key.as_bytes(), rand::random()) as usize % topic_metadata.partitions.len()
        }
        None => rand::random_range(0..topic_metadata.partitions.len()),
    };

    let partition = &topic_metadata.partitions[index];

    // 3. Lookup leader replica for partition

    let record = Record::new(
        0,
        args.key.ok_or("".to_string()).unwrap().as_bytes(),
        args.value.as_bytes(),
    )
    .encode();
    let record_batch = RecordBatch {
        base_offset: 0,
        batch_length: record.len() as u32,
        records_count: 1,
        records: record.into(),
    };

    let body = ProduceRequest {
        transactional_id: 0,
        acks: Acks::Leader,
        timeout: Duration::new(5, 0),
        topics: vec![ProduceTopic {
            topic: args.topic,
            partitions: vec![ProducePartition {
                index: partition.partition_index as u16,
                records: record_batch,
            }],
        }],
    };

    let produce_request = Frame::new(ApiKey::Produce, FrameBody::Produce(body));

    // 4. Send `ProduceRequest`
    stream.write_all(&produce_request.encode()).await.unwrap();

    // 5. Await for response
    let response_size = stream.read_u32().await.unwrap();
    let mut buf = BytesMut::with_capacity(response_size as usize);

    stream.read_exact(&mut buf).await.unwrap();

    let produce_response = Frame::decode(&buf.freeze(), response_size).unwrap();
    println!("Successfully wrote: {produce_response:#?} into broker");
}
