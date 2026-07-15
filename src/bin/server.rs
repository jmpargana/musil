use clap::Parser;
use std::sync::Arc;

use rafka::{broker::Broker, network::server::SocketServer};

#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    #[arg(short, long, default_value = "./data")]
    path: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let broker = Broker::new(args.path);
    let srv = SocketServer::new(Arc::new(broker));
    srv.listen().await;
}
