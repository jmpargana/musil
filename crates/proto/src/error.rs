use tokio::io;

#[derive(Debug)]
pub enum ProtoError {
    CRC,
    Io(io::Error),
}
