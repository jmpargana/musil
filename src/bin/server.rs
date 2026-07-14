use std::sync::Arc;

use rafka::{broker::Broker, network::server::SocketServer};

#[tokio::main]
async fn main() {
    let broker = Broker::new();
    let srv = SocketServer::new(Arc::new(broker));
    srv.listen().await;
}
