pub struct FetchPartition {
    pub partition: u32,
    pub fetch_offset: u64,
    // // This field is only used when the request is sent by the follower.
    // // TODO: need to figure out how to encode.
    // pub log_start_offset: Option<u64>,
    // This limit may not be honored.
    pub partition_max_bytes: u32,
    // This field is only used when the request is sent by the follower.
    // TODO: need to figure out how to encode.
    // For now hardcoding to 0
    pub high_watermark: u64,
}
