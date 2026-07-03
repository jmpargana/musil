use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};

mod segment;
mod record;
mod partition;
mod broker;
mod topic;

#[tokio::main]
async fn main() {
    
    println!("Hello, world!");
}
