use std::{io::Error, sync::Arc};

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    broker::Broker,
    protocol::{Frame, codec::ParseError},
};

pub struct Connection {
    pub stream: TcpStream,
    pub broker: Arc<Broker>,
    // Reusable buffer for multiple streams in same connection to avoid allocating.
    // Not sure how to measure this...
    pub read_buf: BytesMut,
}

#[derive(Debug)]
pub enum ConnectionError {
    Io(Error),
    Protocol(ParseError),
    // internal errors are returned inside response body.
}

impl Connection {
    pub async fn handle(mut self) {
        loop {
            // TODO: implement error handling
            let request = match self.read_frame().await {
                Ok(r) => r,
                Err(ConnectionError::Io(_)) => {
                    break;
                }
                Err(ConnectionError::Protocol(e)) => {
                    tracing::warn!("protocol error: {e:?}");
                    break;
                }
            };
            let response = match self.broker.handle(request).await {
                Ok(r) => r,
                Err(e) => unreachable!(
                    "this needs to be teste. the response should already include a error code from the broker"
                ),
            };

            if let Err(e) = self.write_frame(response).await {
                // TODO: add tracing everywhere
                tracing::warn!("write failed: {e:?}");
                break;
            }
        }
    }

    async fn read_frame(&mut self) -> Result<Frame, ConnectionError> {
        let size = self
            .stream
            .read_u32()
            .await
            .map_err(|e| ConnectionError::Io(e))?;

        self.read_buf.resize(size as usize, 0);

        self.stream
            .read_exact(&mut self.read_buf)
            .await
            .map_err(|e| ConnectionError::Io(e))?;

        let frame = Frame::decode(&self.read_buf.split().freeze(), size)
            .map_err(|e| ConnectionError::Protocol(e))?;
        Ok(frame)
    }

    async fn write_frame(&mut self, res: Frame) -> Result<(), ConnectionError> {
        let bytes = res.encode();
        self.stream
            .write_all(&bytes)
            .await
            .map_err(|e| ConnectionError::Io(e))
    }
}
