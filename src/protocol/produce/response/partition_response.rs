pub struct CurrentLeader {
    pub leader_id: i32,
    pub leader_epoch: u32,
}

pub struct ProducePartitionResponse {
    pub index: u32, // partition id
    pub error_code: u16,
    pub base_offset: u64, // where log was appended
    pub log_append_time_ms: u64,
    pub log_start_offset: u64, // used to read or recover old offsets
    // record_errors
    pub error_message: String,
    pub current_leader: Option<CurrentLeader>,
}
