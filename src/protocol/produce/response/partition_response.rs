use crate::protocol::error_codes::ErrorCode;

#[derive(Debug, Clone)]
pub struct CurrentLeader {
    pub leader_id: i32,
    pub leader_epoch: u32,
}

#[derive(Debug, Clone)]
pub struct ProducePartitionResponse {
    pub index: u32, // partition id
    pub error_code: ErrorCode,
    pub base_offset: u64, // where log was appended
    pub log_append_time_ms: u64,
    pub log_start_offset: u64, // used to read or recover old offsets
    // record_errors
    pub error_message: String,
    pub current_leader: Option<CurrentLeader>,
}

impl ProducePartitionResponse {
    pub fn new(index: u32, base_offset: u64, broker_id: i32) -> Self {
        // TODO: fill missing fields
        Self {
            index,
            error_code: ErrorCode::None,
            base_offset,
            log_append_time_ms: 0,
            log_start_offset: 0,
            error_message: "".to_string(),
            current_leader: Some(CurrentLeader {
                leader_id: broker_id,
                leader_epoch: 0,
            }),
        }
    }

    pub fn error(index: u32, error_code: ErrorCode) -> Self {
        Self {
            index,
            error_code,
            base_offset: 0,
            log_append_time_ms: 0,
            log_start_offset: 0,
            error_message: "".to_string(),
            current_leader: None,
        }
    }

    pub(crate) fn get_size(&self) -> u32 {
        // index(4) + error_code(2) + base_offset(8) + log_append_time_ms(8) + log_start_offset(8)
        // + error_message_len_prefix(2) + error_message_bytes
        // + current_leader_flag(1) + optional leader(i32+u32 = 8)
        let current_leader_size = if self.current_leader.is_some() { 1 + 8 } else { 1 };
        4 + 2 + 8 + 8 + 8 + 2 + self.error_message.len() as u32 + current_leader_size
    }
}
