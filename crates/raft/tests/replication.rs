use raft::*;

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

fn make_leader() -> Node<TestLog> {
    let log = TestLog::with_entries(vec![LogEntry {
        epoch: 1,
        offset: 0,
        data: vec![1],
    }]);
    let mut node = Node::new(1, vec![1, 2, 3], QuorumState::empty(), log);
    node.handle(Event::ElectionTimeout);
    node.handle(Event::VoteResponse {
        from: 2,
        resp: VoteResponse {
            epoch: 1,
            granted: true,
        },
    });
    assert_eq!(node.role(), Role::Leader);
    node
}

#[test]
fn leader_rejects_propose_when_follower() {
    let mut node = Node::new(1, vec![1, 2, 3], QuorumState::empty(), TestLog::new());
    let actions = node.handle(Event::Propose {
        data: vec![42],
        propose_id: 1,
    });
    assert_eq!(actions, vec![Action::RejectPropose(1)]);
}

#[test]
fn leader_appends_on_propose() {
    let mut node = make_leader();
    let actions = node.handle(Event::Propose {
        data: vec![42],
        propose_id: 1,
    });

    assert_eq!(
        actions,
        vec![Action::AppendToLog(LogEntry {
            epoch: 1,
            offset: 1,
            data: vec![42],
        })]
    );
}

#[test]
fn leader_serves_fetch_request() {
    let mut node = make_leader();
    let actions = node.handle(Event::FetchRequest {
        from: 2,
        req: FetchRequest {
            epoch: 1,
            fetch_offset: 0,
            last_fetched_epoch: 0,
        },
    });

    let response = actions.iter().find_map(|a| match a {
        Action::SendFetchResponse(to, resp) if *to == 2 => Some(resp),
        _ => None,
    });
    assert!(response.is_some());
    let resp = response.unwrap();
    assert_eq!(resp.epoch, 1);
    assert_eq!(resp.entries.len(), 1);
}

#[test]
fn follower_applies_entries_from_fetch_response() {
    let mut node = Node::new(1, vec![1, 2, 3], QuorumState::new(1, None), TestLog::new());

    let actions = node.handle(Event::FetchResponse {
        from: 2,
        resp: FetchResponse {
            epoch: 1,
            high_watermark: 0,
            entries: vec![LogEntry {
                epoch: 1,
                offset: 0,
                data: vec![1],
            }],
            diverging: None,
        },
    });

    let appends: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, Action::AppendToLog(_)))
        .collect();
    assert_eq!(appends.len(), 1);
    assert!(actions.contains(&Action::ResetElectionTimer));
}

#[test]
fn follower_truncates_on_divergence() {
    let mut node = Node::new(1, vec![1, 2, 3], QuorumState::new(1, None), TestLog::new());

    let actions = node.handle(Event::FetchResponse {
        from: 2,
        resp: FetchResponse {
            epoch: 1,
            high_watermark: 0,
            entries: vec![],
            diverging: Some(Diverging {
                epoch: 0,
                end_offset: 0,
            }),
        },
    });

    assert_eq!(actions, vec![Action::TruncateLog(0)]);
}

#[test]
fn heartbeat_timeout_sends_to_followers() {
    let mut node = make_leader();
    let actions = node.handle(Event::HeartbeatTimeout);

    assert!(actions.contains(&Action::ResetHeartbeatTimer));
    let sends: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, Action::SendFetchResponse(_, _)))
        .collect();
    assert_eq!(sends.len(), 2);
}
