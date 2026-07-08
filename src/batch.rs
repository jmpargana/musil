use crate::record::Record;

type RawBatch = Vec<u8>;

// TODO: there's way more information here. I'm starting with the basic
pub struct Batch {
    base_offset: u64,
    batch_length: u32, // how many bytes follow (including fields until records)
    records_count: u32,
    records: Vec<Record>,
}
