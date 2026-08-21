use std::{fs, path::Path};

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

    pub fn load(path: &Path) -> Self {
        match fs::read_to_string(path) {
            Ok(contents) => serde_json::from_str(&contents).unwrap_or(Self::empty()),
            Err(_) => Self::empty(),
        }
    }

    pub fn persist(&self, path: &Path) {
        let tmp = path.with_extension("tmp");
        let data = serde_json::to_vec(self).expect("QuorumState serialization failed");
        fs::write(&tmp, &data).expect("QuorumState write failed");
        let f = fs::File::open(&tmp).expect("QuorumState fsync open failed");
        f.sync_all().expect("QuorumState fsync failed");
        fs::rename(&tmp, path).expect("QuorumState rename failed");
        if let Some(dir) = path.parent()
            && let Ok(d) = fs::File::open(dir)
        {
            let _ = d.sync_all();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}
