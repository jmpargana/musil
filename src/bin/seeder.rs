use bytes::BytesMut;
use clap::Parser;
use network::protocol::{Frame, body::FrameBody, header::ApiKey, metadata::CreateTopicRequest};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(short, long, value_delimiter = ',', num_args = 1.., default_value = "127.0.0.1:9092")]
    bootstrap_servers: Vec<String>,

    #[arg(short, long, default_value = "seeder.toml")]
    file: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

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
