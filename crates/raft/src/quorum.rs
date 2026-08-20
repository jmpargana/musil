// crates/raft/src/quorum.rs
pub struct Quorum {
    voters: Vec<u16>,
}

impl Quorum {
    pub fn new(voters: Vec<u16>) -> Self {
        Self { voters }
    }

    pub fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    pub fn is_majority(&self, count: usize) -> bool {
        count >= self.majority()
    }

    /// Given voter offsets, return the highest offset replicated on a majority.
    /// This is the "median" approach — sort descending, take the majority-th value.
    pub fn majority_offset(&self, offsets: &HashMap<u16, u64>, self_offset: u64) -> u64 {
        let mut sorted: Vec<u64> = self
            .voters
            .iter()
            .filter_map(|id| offsets.get(id).copied())
            .collect();
        sorted.push(self_offset); // leader's own offset
        sorted.sort_unstable_by(|a, b| b.cmp(a));

        if sorted.len() >= self.majority() {
            sorted[self.majority() - 1]
        } else {
            0
        }
    }
}
