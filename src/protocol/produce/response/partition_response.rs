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

    // TODO: maybe option should have an additional byte or 2 to represent (tag0).
    pub(crate) fn get_size(&self) -> u32 {
        let current_leader_size = if let Some(_) = &self.current_leader {
            8
        } else {
            0
        };
        4 + 2 + 8 + 8 + 8 + self.error_message.len() as u32 + current_leader_size
    }
}
