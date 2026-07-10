use std::sync::Arc;

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{broker::Broker, protocol::Frame};

pub struct Connection {
    pub stream: TcpStream,
    pub broker: Arc<Broker>,
}

impl Connection {
    pub async fn handle(mut self) {
        // FIXME: this is most likely not correct. How can we check the size of the request
        let mut bytes = BytesMut::new();
        self.stream.read(&mut bytes).await.unwrap();
        let frame = Frame::decode(bytes.freeze()).unwrap();
        let res = self.broker.handle(frame).await.unwrap();
        let bytes = res.encode();
        self.stream.write_all(&bytes).await.unwrap();
    }
}
