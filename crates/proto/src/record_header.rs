#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RecordHeader {
    key: Vec<u8>,
    value: Vec<u8>,
}
