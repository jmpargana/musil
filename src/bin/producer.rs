use clap::Parser;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    // TODO: directly parse comma-seperated values including host+port config.
    #[arg(short, long)]
    bootstrap_servers: String,

    #[arg(short, long)]
    topic: String,

    #[arg(short, long)]
    key: Option<String>,

    #[arg(short, long)]
    value: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!("Checking {}", args.bootstrap_servers)

    // 1. Call metadata against bootstrap server
    // 2. Perform hash or pick random partition
    // 3. Lookup leader replica for partition
    // 4. Send `ProduceRequest`
    // 5. Await for response
}
