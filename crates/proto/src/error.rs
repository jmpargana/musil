use std::io;

#[derive(Debug)]
pub enum ProtoError {
    CRC,
    InvalidOffset,
    Io(io::Error),
}
