use crate::action::Action;
use crate::log::RaftLog;
use crate::node::Node;
use crate::rpc::{BeginQuorumEpochRequest, EndQuorumEpochRequest, VoteRequest, VoteResponse};
use crate::state::Role;

impl<L: RaftLog> Node<L> {
    pub(crate) fn handle_election_timeout(&mut self) -> Vec<Action> {
        match self.role {
            Role::Leader => return vec![],
            _ => {}
        }

        self.current_epoch += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.votes_received.clear();
        self.votes_received.insert(self.id);

        let mut actions = vec![
            Action::PersistQuorumState,
            Action::ResetElectionTimer,
        ];

        if self.votes_received.len() >= self.majority() {
            actions.extend(self.become_leader());
            return actions;
        }

        let last_log_epoch = self.log.last_epoch();
        let last_log_offset = self.log.log_end_offset();

        let req = VoteRequest {
            epoch: self.current_epoch,
            candidate_id: self.id,
            last_log_epoch,
            last_log_offset,
        };

        for &voter in &self.voters.clone() {
            if voter != self.id {
                actions.push(Action::SendVote(voter, req.clone()));
            }
        }

        actions
    }

    pub(crate) fn handle_vote_request(&mut self, from: u16, req: VoteRequest) -> Vec<Action> {
        let mut actions = Vec::new();

        if req.epoch > self.current_epoch {
            self.become_follower(req.epoch, None);
            actions.push(Action::PersistQuorumState);
        }

        if req.epoch < self.current_epoch {
            actions.push(Action::SendVoteResponse(
                from,
                VoteResponse {
                    epoch: self.current_epoch,
                    granted: false,
                },
            ));
            return actions;
        }

        let can_vote = self.voted_for.is_none() || self.voted_for == Some(from);
        let log_ok = self.is_log_up_to_date(req.last_log_epoch, req.last_log_offset);

        if can_vote && log_ok {
            self.voted_for = Some(from);
            actions.push(Action::PersistQuorumState);
            actions.push(Action::ResetElectionTimer);
            actions.push(Action::SendVoteResponse(
                from,
                VoteResponse {
                    epoch: self.current_epoch,
                    granted: true,
                },
            ));
        } else {
            actions.push(Action::SendVoteResponse(
                from,
                VoteResponse {
                    epoch: self.current_epoch,
                    granted: false,
                },
            ));
        }

        actions
    }

    pub(crate) fn handle_vote_response(&mut self, from: u16, resp: VoteResponse) -> Vec<Action> {
        if resp.epoch > self.current_epoch {
            self.become_follower(resp.epoch, None);
            return vec![Action::PersistQuorumState];
        }

        if self.role != Role::Candidate || resp.epoch != self.current_epoch {
            return vec![];
        }

        if resp.granted {
            self.votes_received.insert(from);
            if self.votes_received.len() >= self.majority() {
                return self.become_leader();
            }
        }

        vec![]
    }

    pub(crate) fn handle_begin_quorum_epoch(
        &mut self,
        _from: u16,
        req: BeginQuorumEpochRequest,
    ) -> Vec<Action> {
        let mut actions = Vec::new();

        if req.epoch >= self.current_epoch {
            self.become_follower(req.epoch, Some(req.leader_id));
            actions.push(Action::PersistQuorumState);
            actions.push(Action::ResetElectionTimer);
            actions.extend(self.reject_pending_proposes());
        }

        actions
    }

    pub(crate) fn handle_end_quorum_epoch(
        &mut self,
        _from: u16,
        req: EndQuorumEpochRequest,
    ) -> Vec<Action> {
        if req.epoch >= self.current_epoch {
            self.become_follower(req.epoch, None);
            let mut actions = vec![
                Action::PersistQuorumState,
                Action::ResetElectionTimer,
            ];
            actions.extend(self.reject_pending_proposes());
            return actions;
        }
        vec![]
    }

    fn become_leader(&mut self) -> Vec<Action> {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.voter_fetch_offsets.clear();

        for &voter in &self.voters {
            if voter != self.id {
                self.voter_fetch_offsets.insert(voter, 0);
            }
        }

        let targets: Vec<u16> = self.voters.iter().copied().filter(|&v| v != self.id).collect();

        vec![
            Action::ResetHeartbeatTimer,
            Action::SendBeginQuorumEpoch(
                targets,
                BeginQuorumEpochRequest {
                    epoch: self.current_epoch,
                    leader_id: self.id,
                },
            ),
        ]
    }

    fn is_log_up_to_date(&self, candidate_epoch: u32, candidate_offset: u64) -> bool {
        let my_epoch = self.log.last_epoch();
        let my_offset = self.log.log_end_offset();

        if candidate_epoch != my_epoch {
            candidate_epoch > my_epoch
        } else {
            candidate_offset >= my_offset
        }
    }
}
