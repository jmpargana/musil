use bytes::BytesMut;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::protocol::fetch::request::fetch_partition::FetchPartition;
use crate::protocol::fetch::request::fetch_request::FetchRequest;
use crate::protocol::fetch::request::fetch_topic::FetchTopic;
use crate::protocol::fetch::response::fetch_response::FetchResponse;
use crate::protocol::{
    Frame,
    body::FrameBody,
    header::ApiKey,
    metadata::{MetadataRequest, MetadataResponse},
};

#[derive(Debug)]
pub enum RequestError {
    IoErr(std::io::Error),
    ParseErr,
    FormatErr,
}

pub async fn request_metadata(stream: &mut TcpStream) -> Result<MetadataResponse, RequestError> {
    let body = MetadataRequest {
        topics: vec![],
        allow_auto_topic_creation: true,
    };
    let frame = Frame::new(ApiKey::Metadata, FrameBody::Metadata(body));

    stream
        .write_all(&frame.encode())
        .await
        .map_err(RequestError::IoErr)?;

    let response_size = stream.read_u32().await.map_err(RequestError::IoErr)?;
    let mut buf = BytesMut::zeroed(response_size as usize);

    stream
        .read_exact(&mut buf)
        .await
        .map_err(RequestError::IoErr)?;

    let response_frame = Frame::decode_response(&buf.freeze(), response_size)
        .map_err(|_| RequestError::ParseErr)?;

    response_frame
        .body
        .try_into()
        .map_err(|_| RequestError::FormatErr)
}

pub async fn request_fetch(
    stream: &mut TcpStream,
    topic: String,
    partition: u32,
    fetch_offset: u64,
) -> Result<FetchResponse, RequestError> {
    let body = FetchRequest {
        replica_id: -1,
        max_bytes: 4096,
        topics: vec![FetchTopic {
            topic,
            partitions: vec![FetchPartition {
                partition,
                fetch_offset,
                partition_max_bytes: 4096,
                high_watermark: 0,
            }],
        }],
    };

    let frame = Frame::new(ApiKey::Fetch, FrameBody::Fetch(body));

    stream
        .write_all(&frame.encode())
        .await
        .map_err(RequestError::IoErr)?;

    let response_size = stream.read_u32().await.map_err(RequestError::IoErr)?;
    let mut buf = BytesMut::zeroed(response_size as usize);

    stream.read_exact(&mut buf).await.map_err(RequestError::IoErr)?;

    let response_frame = Frame::decode_response(&buf.freeze(), response_size)
        .map_err(|_| RequestError::ParseErr)?;

    let FrameBody::FetchResponse(response) = response_frame.body else {
        panic!("expected FetchResponse")
    };

    Ok(response)
}
