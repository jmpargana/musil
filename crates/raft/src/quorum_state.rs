use std::{fs, path::Path};

pub struct QuorumState {
    pub current_epoch: u64,
    pub voted_for: Option<u16>,
}

impl QuorumState {
    pub fn load(path: &Path) -> Self {
        match fs::read_to_string(path) {
            Ok(contents) => serde_json::from_str(&contents).unwrap_or(Self::empty()),
            Err(_) => Self::empty(),
        }
    }

    pub fn empty() -> Self {
        Self {
            current_epoch: 0,
            voted_for: None,
        }
    }

    /// Atomic write: write to temp, fsync, rename.
    pub fn persist(&self, path: &Path) {
        let tmp = path.with_extension("tmp");
        let data = serde_json::to_vec(self).unwrap();
        fs::write(&tmp, &data).unwrap();
        let f = fs::File::open(&tmp).unwrap();
        f.sync_all().unwrap();
        fs::rename(&tmp, path).unwrap();
        let dir = path.parent().unwrap();
        let d = fs::File::open(dir).unwrap();
        d.sync_all().unwrap();
    }
}
