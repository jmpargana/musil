use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, UnixSocket},
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

    let ln = TcpListener::bind("0.0.0.0:8088").await.unwrap();
    let a = UnixSocket::new_stream().unwrap();

    loop {
        let (stream, _) = ln.accept().await.unwrap();
    }
}
