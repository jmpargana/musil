use raft::*;

fn three_node_follower() -> Node<TestLog> {
    Node::new(1, vec![1, 2, 3], QuorumState::empty(), TestLog::new())
}

fn three_node_with_epoch(epoch: u32, voted_for: Option<u16>) -> Node<TestLog> {
    Node::new(
        1,
        vec![1, 2, 3],
        QuorumState::new(epoch, voted_for),
        TestLog::new(),
    )
}

#[test]
fn election_timeout_starts_election() {
    let mut node = three_node_follower();
    let actions = node.handle(Event::ElectionTimeout);

    assert_eq!(node.current_epoch(), 1);
    assert_eq!(node.role(), Role::Candidate);
    assert_eq!(node.voted_for(), Some(1));

    assert!(actions.contains(&Action::PersistQuorumState));
    assert!(actions.contains(&Action::ResetElectionTimer));

    let vote_sends: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, Action::SendVote(_, _)))
        .collect();
    assert_eq!(vote_sends.len(), 2);
}

#[test]
fn single_node_wins_immediately() {
    let mut node = Node::new(1, vec![1], QuorumState::empty(), TestLog::new());
    let actions = node.handle(Event::ElectionTimeout);

    assert_eq!(node.role(), Role::Leader);
    assert!(actions.contains(&Action::ResetHeartbeatTimer));
}

#[test]
fn candidate_wins_with_majority() {
    let mut node = three_node_follower();
    node.handle(Event::ElectionTimeout);

    let actions = node.handle(Event::VoteResponse {
        from: 2,
        resp: VoteResponse {
            epoch: 1,
            granted: true,
        },
    });

    assert_eq!(node.role(), Role::Leader);
    assert!(actions.contains(&Action::ResetHeartbeatTimer));
}

#[test]
fn candidate_does_not_win_without_majority() {
    let mut node = Node::new(1, vec![1, 2, 3, 4, 5], QuorumState::empty(), TestLog::new());
    node.handle(Event::ElectionTimeout);

    let actions = node.handle(Event::VoteResponse {
        from: 2,
        resp: VoteResponse {
            epoch: 1,
            granted: true,
        },
    });

    assert_eq!(node.role(), Role::Candidate);
    assert!(actions.is_empty());
}

#[test]
fn follower_grants_vote_if_epoch_higher() {
    let mut node = three_node_follower();
    let actions = node.handle(Event::VoteRequest {
        from: 2,
        req: VoteRequest {
            epoch: 1,
            candidate_id: 2,
            last_log_epoch: 0,
            last_log_offset: 0,
        },
    });

    assert_eq!(node.current_epoch(), 1);
    assert_eq!(node.voted_for(), Some(2));
    assert!(actions.contains(&Action::SendVoteResponse(
        2,
        VoteResponse {
            epoch: 1,
            granted: true,
        }
    )));
}

#[test]
fn follower_rejects_vote_if_already_voted_for_other() {
    let mut node = three_node_with_epoch(1, Some(3));
    let actions = node.handle(Event::VoteRequest {
        from: 2,
        req: VoteRequest {
            epoch: 1,
            candidate_id: 2,
            last_log_epoch: 0,
            last_log_offset: 0,
        },
    });

    assert!(actions.contains(&Action::SendVoteResponse(
        2,
        VoteResponse {
            epoch: 1,
            granted: false,
        }
    )));
}

#[test]
fn follower_rejects_vote_if_log_not_up_to_date() {
    let log = TestLog::with_entries(vec![LogEntry {
        epoch: 1,
        offset: 0,
        data: vec![],
    }]);
    let mut node = Node::new(1, vec![1, 2, 3], QuorumState::new(1, None), log);

    let actions = node.handle(Event::VoteRequest {
        from: 2,
        req: VoteRequest {
            epoch: 2,
            candidate_id: 2,
            last_log_epoch: 0,
            last_log_offset: 0,
        },
    });

    assert!(actions.contains(&Action::SendVoteResponse(
        2,
        VoteResponse {
            epoch: 2,
            granted: false,
        }
    )));
}

#[test]
fn higher_epoch_causes_step_down() {
    let mut node = three_node_follower();
    node.handle(Event::ElectionTimeout);
    assert_eq!(node.role(), Role::Candidate);

    node.handle(Event::VoteResponse {
        from: 2,
        resp: VoteResponse {
            epoch: 5,
            granted: false,
        },
    });

    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.current_epoch(), 5);
}

#[test]
fn begin_quorum_epoch_makes_follower() {
    let mut node = three_node_follower();
    node.handle(Event::ElectionTimeout);

    let actions = node.handle(Event::BeginQuorumEpoch {
        from: 2,
        req: BeginQuorumEpochRequest {
            epoch: 1,
            leader_id: 2,
        },
    });

    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.leader_id(), Some(2));
    assert!(actions.contains(&Action::PersistQuorumState));
    assert!(actions.contains(&Action::ResetElectionTimer));
}

// Test helpers — public VecLog equivalent for integration tests
pub struct TestLog {
    entries: Vec<LogEntry>,
}

impl TestLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn with_entries(entries: Vec<LogEntry>) -> Self {
        Self { entries }
    }
}

impl RaftLog for TestLog {
    fn log_end_offset(&self) -> u64 {
        self.entries.last().map(|e| e.offset + 1).unwrap_or(0)
    }

    fn epoch_at(&self, offset: u64) -> Option<u32> {
        self.entries.iter().find(|e| e.offset == offset).map(|e| e.epoch)
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
}
