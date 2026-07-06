use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

mod broker;
mod command;
mod partition;
mod record;
mod replica;
mod segment;
mod topic;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
}
