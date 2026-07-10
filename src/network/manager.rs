use std::sync::Arc;

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{broker::Broker, message::Message};

pub struct NetworkManager {
    broker: Arc<Broker>,
}

// Copies broker reference from manager
pub struct Connection {
    stream: TcpStream,
    broker: Arc<Broker>,
}

impl NetworkManager {
    pub async fn listen(&self) {
        let ln = TcpListener::bind("0.0.0.0:8088").await.unwrap();
        loop {
            let (stream, _) = ln.accept().await.unwrap();
            let mut conn = Connection {
                stream,
                broker: self.broker.clone(),
            };
            tokio::spawn(async move {
                // FIXME: this is most likely not correct. How can we check the size of the request
                let mut bytes = BytesMut::new();
                conn.stream.read(&mut bytes).await.unwrap();
                let msg = Message::decode(bytes.freeze()).unwrap();
                let res = conn.broker.handle(msg).await.unwrap();
                let bytes = res.encode();
                conn.stream.write_all(&bytes).await.unwrap();
            });
        }
    }
}
