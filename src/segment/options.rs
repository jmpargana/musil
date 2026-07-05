use derive_builder::Builder;

#[derive(Builder)]
pub struct SegmentConfig {
    // TODO: maybe this could revert to &str
    pub base_dir: String,
    pub base_offset: u64,
    #[builder(default = 1<<20)]
    pub segment_bytes: usize,
    #[builder(default = 1<<12)]
    pub index_interval_bytes: usize,
}
