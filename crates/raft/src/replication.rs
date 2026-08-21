use crate::{
    action::Action,
    log::{LogEntry, RaftLog},
    node::Node,
    rpc::{Diverging, FetchRequest, FetchResponse},
    state::Role,
};

impl<L: RaftLog> Node<L> {
    pub(crate) fn handle_fetch_request(&mut self, from: u16, req: FetchRequest) -> Vec<Action> {
        if req.epoch > self.current_epoch {
            self.become_follower(req.epoch, None);
            return vec![
                Action::PersistQuorumState,
                Action::SendFetchResponse(
                    from,
                    FetchResponse {
                        epoch: self.current_epoch,
                        high_watermark: self.high_watermark,
                        entries: vec![],
                        diverging: None,
                    },
                ),
            ];
        }

        if req.epoch < self.current_epoch {
            return vec![Action::SendFetchResponse(
                from,
                FetchResponse {
                    epoch: self.current_epoch,
                    high_watermark: self.high_watermark,
                    entries: vec![],
                    diverging: None,
                },
            )];
        }

        if self.role != Role::Leader {
            return vec![Action::SendFetchResponse(
                from,
                FetchResponse {
                    epoch: self.current_epoch,
                    high_watermark: self.high_watermark,
                    entries: vec![],
                    diverging: None,
                },
            )];
        }

        let diverging = self.check_divergence(req.fetch_offset, req.last_fetched_epoch);

        if diverging.is_some() {
            return vec![Action::SendFetchResponse(
                from,
                FetchResponse {
                    epoch: self.current_epoch,
                    high_watermark: self.high_watermark,
                    entries: vec![],
                    diverging,
                },
            )];
        }

        let entries = self
            .log
            .entries(req.fetch_offset, self.log.log_end_offset());

        if self.voters.contains(&from) {
            self.voter_fetch_offsets.insert(from, req.fetch_offset);
        }

        let mut actions = vec![Action::SendFetchResponse(
            from,
            FetchResponse {
                epoch: self.current_epoch,
                high_watermark: self.high_watermark,
                entries,
                diverging: None,
            },
        )];

        actions.extend(self.try_advance_high_watermark());

        actions
    }

    pub(crate) fn handle_fetch_response(&mut self, _from: u16, resp: FetchResponse) -> Vec<Action> {
        if resp.epoch > self.current_epoch {
            self.become_follower(resp.epoch, None);
            let mut actions = vec![Action::PersistQuorumState];
            actions.extend(self.reject_pending_proposes());
            return actions;
        }

        if self.role != Role::Follower {
            return vec![];
        }

        let mut actions = Vec::new();

        if let Some(diverging) = resp.diverging {
            actions.push(Action::TruncateLog(diverging.end_offset));
            return actions;
        }

        for entry in &resp.entries {
            actions.push(Action::AppendToLog(entry.clone()));
        }

        if resp.high_watermark > self.high_watermark {
            let new_hwm = resp.high_watermark.min(self.log.log_end_offset());
            self.high_watermark = new_hwm;
            actions.push(Action::AdvanceHighWatermark(new_hwm));
            actions.extend(self.check_committed_proposes());
        }

        actions.push(Action::ResetElectionTimer);

        actions
    }

    pub(crate) fn handle_heartbeat_timeout(&mut self) -> Vec<Action> {
        if self.role != Role::Leader {
            return vec![];
        }

        let mut actions = vec![Action::ResetHeartbeatTimer];

        for &voter in &self.voters.clone() {
            if voter != self.id {
                let fetch_offset = self.voter_fetch_offsets.get(&voter).copied().unwrap_or(0);
                let entries = self.log.entries(fetch_offset, self.log.log_end_offset());
                actions.push(Action::SendFetchResponse(
                    voter,
                    FetchResponse {
                        epoch: self.current_epoch,
                        high_watermark: self.high_watermark,
                        entries,
                        diverging: None,
                    },
                ));
            }
        }

        actions
    }

    pub(crate) fn handle_propose(&mut self, data: Vec<u8>, propose_id: u64) -> Vec<Action> {
        if self.role != Role::Leader {
            return vec![Action::RejectPropose(propose_id)];
        }

        let offset = self.log.log_end_offset();
        let entry = LogEntry {
            epoch: self.current_epoch,
            offset,
            data,
        };

        self.pending_proposes.push((offset, propose_id));

        vec![Action::AppendToLog(entry)]
    }

    fn check_divergence(&self, fetch_offset: u64, last_fetched_epoch: u32) -> Option<Diverging> {
        if fetch_offset == 0 {
            return None;
        }

        let prev_offset = fetch_offset - 1;
        match self.log.epoch_at(prev_offset) {
            Some(epoch) if epoch == last_fetched_epoch => None,
            Some(_) | None => {
                let end_offset = self.log.find_epoch_start(last_fetched_epoch);
                Some(Diverging {
                    epoch: last_fetched_epoch,
                    end_offset,
                })
            }
        }
    }

    fn try_advance_high_watermark(&mut self) -> Vec<Action> {
        let my_offset = self.log.log_end_offset();

        let mut offsets: Vec<u64> = self.voter_fetch_offsets.values().copied().collect();
        offsets.push(my_offset);
        offsets.sort_unstable_by(|a, b| b.cmp(a));

        let new_hwm = if offsets.len() >= self.majority() {
            offsets[self.majority() - 1]
        } else {
            return vec![];
        };

        if new_hwm <= self.high_watermark {
            return vec![];
        }

        if let Some(epoch) = self.log.epoch_at(new_hwm - 1)
            && epoch != self.current_epoch
        {
            return vec![];
        }

        self.high_watermark = new_hwm;
        let mut actions = vec![Action::AdvanceHighWatermark(new_hwm)];
        actions.extend(self.check_committed_proposes());
        actions
    }

    fn check_committed_proposes(&mut self) -> Vec<Action> {
        let mut actions = Vec::new();
        let mut i = 0;
        while i < self.pending_proposes.len() {
            if self.pending_proposes[i].0 < self.high_watermark {
                let (_, propose_id) = self.pending_proposes.remove(i);
                actions.push(Action::CommitPropose(propose_id));
            } else {
                i += 1;
            }
        }
        actions
    }
}
