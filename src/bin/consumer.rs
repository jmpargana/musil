use bytes::BytesMut;
use clap::Parser;
use storage::protocol::{
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
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let body = FetchRequest {
        replica_id: -1,
        max_bytes: 4096, // FIXME: use arg
        topics: vec![FetchTopic {
            topic: args.topic,
            partitions: vec![FetchPartition {
                partition: args.partition as u32, // FIXME: payload probably needs i32 instead.
                fetch_offset: args.offset,
                partition_max_bytes: 4096,
                // FIXME: figure out why hw is needed in the payload?
                high_watermark: 0,
            }],
        }],
    };

    let fetch_request = Frame::new(ApiKey::Fetch, FrameBody::Fetch(body));

    stream.write_all(&fetch_request.encode()).await.unwrap();

    let response_size = stream.read_u32().await.unwrap();
    let mut buf = BytesMut::zeroed(response_size as usize);

    stream.read_exact(&mut buf).await.unwrap();

    let fetch_response = Frame::decode_response(&buf.freeze(), response_size).unwrap();
    println!("Successfully read: {fetch_response:#?} from broker");
}
