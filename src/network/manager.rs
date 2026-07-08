use std::sync::Arc;

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
};

use crate::{broker::Broker, message::parser::MessageParser};

pub struct NetworkManager {
    broker: Arc<Broker>,
}

// Copies broker reference from manager
pub struct Connection {
    stream: TcpStream,
    broker: Arc<Broker>,
    parser: MessageParser,
}

impl NetworkManager {
    pub async fn listen(&self) {
        let ln = TcpListener::bind("0.0.0.0:8088").await.unwrap();
        loop {
            let (stream, _) = ln.accept().await.unwrap();
            let mut conn = Connection {
                stream,
                broker: self.broker.clone(),
                parser: MessageParser,
            };
            tokio::spawn(async move {
                // FIXME: this is most likely not correct. How can we check the size of the request
                let mut bytes = BytesMut::new();
                conn.stream.read(&mut bytes).await.unwrap();
                let msg = conn.parser.parse(bytes.freeze()).unwrap();
                let res = conn.broker.handle(msg).await.unwrap();
                // TODO: encode response before writing
                conn.stream.write_all(res).await.unwrap();
            });
        }
    }
}
