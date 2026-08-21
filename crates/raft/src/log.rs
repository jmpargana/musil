#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub epoch: u32,
    pub offset: u64,
    pub data: Vec<u8>,
}

impl Default for LogEntry {
    fn default() -> Self {
        Self {
            epoch: Default::default(),
            offset: Default::default(),
            data: Default::default(),
        }
    }
}

pub trait RaftLog {
    fn log_end_offset(&self) -> u64;
    fn epoch_at(&self, offset: u64) -> Option<u32>;
    fn last_epoch(&self) -> u32;
    fn entries(&self, start: u64, end: u64) -> Vec<LogEntry>;
    fn find_epoch_start(&self, epoch: u32) -> u64;

    fn append(&mut self, entry: LogEntry) -> impl std::future::Future<Output = ()> + Send;
    fn truncate(&mut self, offset: u64) -> impl std::future::Future<Output = ()> + Send;
}

#[cfg(test)]
pub struct VecLog {
    entries: Vec<LogEntry>,
}

#[cfg(test)]
impl VecLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn with_entries(entries: Vec<LogEntry>) -> Self {
        Self { entries }
    }
}

#[cfg(test)]
impl RaftLog for VecLog {
    fn log_end_offset(&self) -> u64 {
        self.entries.last().map(|e| e.offset + 1).unwrap_or(0)
    }

    fn epoch_at(&self, offset: u64) -> Option<u32> {
        self.entries
            .iter()
            .find(|e| e.offset == offset)
            .map(|e| e.epoch)
    }

    fn last_epoch(&self) -> u32 {
        self.entries.last().map(|e| e.epoch).unwrap_or(0)
    }

    fn entries(&self, start: u64, end: u64) -> Vec<LogEntry> {
        self.entries
            .iter()
            .filter(|e| e.offset >= start && e.offset < end)
            .cloned()
            .collect()
    }

    fn find_epoch_start(&self, epoch: u32) -> u64 {
        self.entries
            .iter()
            .find(|e| e.epoch == epoch)
            .map(|e| e.offset)
            .unwrap_or(0)
    }

    async fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    async fn truncate(&mut self, offset: u64) {
        self.entries.retain(|e| e.offset < offset);
    }
}
