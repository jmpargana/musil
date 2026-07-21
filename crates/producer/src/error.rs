#[derive(Debug)]
pub enum ProducerError {
    ConnErr,
    IoErr(tokio::io::Error),
    ParseErr,
    FormatErr,
    ClientErr,
    UnknownTopicErr,
    ChanClosed,
    UnknownErr,
}
