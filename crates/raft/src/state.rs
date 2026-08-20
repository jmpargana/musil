use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumState {
    pub current_epoch: u32,
    pub voted_for: Option<u16>,
}

impl QuorumState {
    pub fn new(current_epoch: u32, voted_for: Option<u16>) -> Self {
        Self {
            current_epoch,
            voted_for,
        }
    }

    pub fn empty() -> Self {
        Self {
            current_epoch: 0,
            voted_for: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}
