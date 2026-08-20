use std::collections::{HashMap, HashSet};

use crate::action::Action;
use crate::event::Event;
use crate::log::RaftLog;
use crate::state::{QuorumState, Role};

pub struct Node<L: RaftLog> {
    pub(crate) id: u16,
    pub(crate) voters: Vec<u16>,
    pub(crate) log: L,

    // Persistent
    pub(crate) current_epoch: u32,
    pub(crate) voted_for: Option<u16>,

    // Volatile
    pub(crate) role: Role,
    pub(crate) leader_id: Option<u16>,
    pub(crate) votes_received: HashSet<u16>,

    pub(crate) voter_fetch_offsets: HashMap<u16, u64>,
    pub(crate) high_watermark: u64,

    pub(crate) pending_proposes: Vec<(u64, u64)>,
}

impl<L: RaftLog> Node<L> {
    // Log is a state machine. Responsibility to load and restore quorum state is engine, which in this case is the runner.
    pub fn new(id: u16, voters: Vec<u16>, quorum_state: QuorumState, log: L) -> Self {
        Self {
            id,
            voters,
            log,
            current_epoch: quorum_state.current_epoch,
            voted_for: quorum_state.voted_for,
            role: Role::Follower,
            leader_id: None,
            votes_received: HashSet::new(),
            voter_fetch_offsets: HashMap::new(),
            high_watermark: 0,
            pending_proposes: Vec::new(),
        }
    }

    pub fn handle(&mut self, event: Event) -> Vec<Action> {
        match event {
            Event::ElectionTimeout => self.handle_election_timeout(),
            Event::HeartbeatTimeout => self.handle_heartbeat_timeout(),
            Event::VoteRequest { from, req } => self.handle_vote_request(from, req),
            Event::VoteResponse { from, resp } => self.handle_vote_response(from, resp),
            Event::BeginQuorumEpoch { from, req } => self.handle_begin_quorum_epoch(from, req),
            Event::EndQuorumEpoch { from, req } => self.handle_end_quorum_epoch(from, req),
            Event::FetchRequest { from, req } => self.handle_fetch_request(from, req),
            Event::FetchResponse { from, resp } => self.handle_fetch_response(from, resp),
            Event::Propose { data, propose_id } => self.handle_propose(data, propose_id),
        }
    }

    pub fn quorum_state(&self) -> QuorumState {
        QuorumState {
            current_epoch: self.current_epoch,
            voted_for: self.voted_for,
        }
    }

    pub fn current_epoch(&self) -> u32 {
        self.current_epoch
    }

    pub fn voted_for(&self) -> Option<u16> {
        self.voted_for
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn leader_id(&self) -> Option<u16> {
        self.leader_id
    }

    pub fn high_watermark(&self) -> u64 {
        self.high_watermark
    }

    pub(crate) fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    pub(crate) fn become_follower(&mut self, epoch: u32, leader_id: Option<u16>) {
        self.current_epoch = epoch;
        self.role = Role::Follower;
        self.leader_id = leader_id;
        self.voted_for = None;
        self.votes_received.clear();
    }

    pub(crate) fn reject_pending_proposes(&mut self) -> Vec<Action> {
        self.pending_proposes
            .drain(..)
            .map(|(_, propose_id)| Action::RejectPropose(propose_id))
            .collect()
    }
}
