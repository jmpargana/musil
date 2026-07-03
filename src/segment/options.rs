const DEFAULT_SEGMENT_BYTES: usize = 1_048_576; // 1MB (your comment said 1GB)
const DEFAULT_INTERVAL_BYTES: usize = 4096;

pub struct SegmentOptions<'a> {
    pub base_dir: &'a str,
    pub base_offset: u64,
    pub segment_bytes: usize,
    pub index_interval_bytes: usize,
}

impl<'a> SegmentOptions<'a> {
    pub fn with_defaults(base_dir: &'a str, base_offset: u64) -> Self {
        Self {
            base_dir,
            base_offset,
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            index_interval_bytes: DEFAULT_INTERVAL_BYTES,
        }
    }
}
