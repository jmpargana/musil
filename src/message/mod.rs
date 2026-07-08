use crate::message::{body::MessageBody, header::MessageHeader};

pub mod ack;
pub mod body;
pub mod header;
pub mod parser;
pub mod produce;

pub struct Message {
    pub size: u32,
    pub header: MessageHeader,
    pub body: MessageBody,
}
