use std::sync::Arc;

use broker::{Broker, config::BrokerConfig, network::server::SocketServer};
use clap::Parser;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ControllerConfig {
    controller: BrokerConfig,
    brokers: Vec<BrokerConfig>,
}

#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    #[arg(short, long, default_value = "./data")]
    path: String,

    #[arg(short, long, default_value = "server.toml")]
    config: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .try_init()
        .expect("no logs will be bad");

    let settings = config::Config::builder()
        .add_source(config::File::with_name(&args.config))
        .build()
        .unwrap();

    let config = settings.try_deserialize::<ControllerConfig>().unwrap();

    let broker = Broker::new(args.path, config.controller, config.brokers);
    let srv = SocketServer::new(Arc::new(broker));
    srv.listen().await;
}
