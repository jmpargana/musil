use derive_builder::Builder;

#[derive(Builder)]
pub struct PartitionConfig {
    #[builder(default = 1<<20)]
    pub segment_bytes: usize,
    #[builder(default = 1<<16)]
    pub channel_size: usize,
}
