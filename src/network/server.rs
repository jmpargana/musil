use std::sync::Arc;

use tokio::net::TcpListener;

use crate::{broker::Broker, network::connection::Connection};

pub struct SocketServer {
    broker: Arc<Broker>,
}

impl SocketServer {
    pub async fn listen(&self) {
        let ln = TcpListener::bind("0.0.0.0:8088").await.unwrap();
        loop {
            let (stream, _) = ln.accept().await.unwrap();
            let conn = Connection {
                stream,
                broker: self.broker.clone(),
            };
            tokio::spawn(async move {
                conn.handle().await;
            });
        }
    }
}
