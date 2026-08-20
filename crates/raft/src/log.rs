pub trait RaftLog {
    /// Last offset in the log (= log end offset)
    fn end_offset(&self) -> u64;

    /// Epoch of the entry at the given offset
    fn epoch_at(&self, offset: u64) -> Option<u64>;

    /// The last epoch and the offset where it ends
    fn last_epoch_end_offset(&self) -> (u64, u64);

    /// Where a given epoch ends in our log (for divergence detection)
    fn end_offset_for_epoch(&self, epoch: u64) -> u64;

    /// Append a batch (leader only)
    async fn append(&self, batch: RecordBatch) -> u64;

    /// Truncate log to the given offset (follower divergence)
    async fn truncate(&self, offset: u64);

    /// Read batches starting from offset
    fn read(&self, from: u64, max_bytes: u32) -> Vec<RecordBatch>;
}
