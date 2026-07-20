use derive_builder::Builder;

#[derive(Debug, Builder)]
pub struct ProducerConfig {
    pub bootstrap_servers: Vec<String>,
    pub ms_wait: u64,
    pub max_bytes: u32,
}
