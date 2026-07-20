use crate::protocol::error_codes::ErrorCode;

#[derive(Debug, Clone)]
pub struct CurrentLeader {
    pub leader_id: i32,
    pub leader_epoch: u32,
}

#[derive(Debug, Clone)]
pub struct ProducePartitionResponse {
    pub index: u32,
    pub error_code: ErrorCode,
    pub base_offset: u64,
    pub log_append_time_ms: u64,
    pub log_start_offset: u64,
    pub error_message: String,
    pub current_leader: Option<CurrentLeader>,
}

impl ProducePartitionResponse {
    pub fn new(index: u32, base_offset: u64, broker_id: i32) -> Self {
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
        let current_leader_size = if self.current_leader.is_some() { 1 + 8 } else { 1 };
        4 + 2 + 8 + 8 + 8 + 2 + self.error_message.len() as u32 + current_leader_size
    }
}
