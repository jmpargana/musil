use std::{collections::HashMap, path::PathBuf, time::Duration};

use raft::{Action, Event, Node, QuorumState, RaftLog};
use tokio::{
    sync::{mpsc, oneshot},
    time::{self, Interval},
};

use crate::transport::{RaftMessage, RunnerInput, Transport};

pub type ProposeResult = Result<(), ProposeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader,
    ChannelClosed,
}

pub struct RunnerHandle {
    tx: mpsc::Sender<RunnerInput>,
}

impl RunnerHandle {
    pub async fn propose(&self, data: Vec<u8>) -> ProposeResult {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(RunnerInput::Propose {
                data,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ProposeError::ChannelClosed)?;
        reply_rx.await.map_err(|_| ProposeError::ChannelClosed)?
    }
}

pub struct Runner<L: RaftLog, T: Transport> {
    node: Node<L>,
    log: L,
    transport: T,
    pending_proposes: HashMap<u64, oneshot::Sender<ProposeResult>>,
    next_propose_id: u64,
    quorum_state_path: PathBuf,
    last_applied_hwm: u64,
    election_timeout_range: (u64, u64),
    heartbeat_interval_ms: u64,
}

impl<L: RaftLog + Send, T: Transport> Runner<L, T> {
    pub fn spawn(
        id: u16,
        voters: Vec<u16>,
        log: L,
        transport: T,
        quorum_state_path: PathBuf,
        election_timeout_range: (u64, u64),
        heartbeat_interval_ms: u64,
    ) -> RunnerHandle
    where
        L: Clone + 'static,
        T: 'static,
    {
        let (tx, rx) = mpsc::channel(256);

        let quorum_state = QuorumState::load(&quorum_state_path);
        let node_log = log.clone();
        let node = Node::new(id, voters, quorum_state, node_log);

        let runner = Self {
            node,
            log,
            transport,
            pending_proposes: HashMap::new(),
            next_propose_id: 0,
            quorum_state_path,
            last_applied_hwm: 0,
            election_timeout_range,
            heartbeat_interval_ms,
        };

        tokio::spawn(runner.run(rx));

        RunnerHandle { tx }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<RunnerInput>) {
        let election_timeout = time::sleep(self.random_election_duration());
        tokio::pin!(election_timeout);

        let mut heartbeat = time::interval(Duration::from_millis(self.heartbeat_interval_ms));
        heartbeat.tick().await;

        loop {
            tokio::select! {
                input = rx.recv() => {
                    let Some(input) = input else { break };
                    let event = self.input_to_event(input);
                    let actions = self.node.handle(event);
                    self.execute_actions(actions, &mut election_timeout, &mut heartbeat).await;
                }
                _ = &mut election_timeout => {
                    let actions = self.node.handle(Event::ElectionTimeout);
                    self.execute_actions(actions, &mut election_timeout, &mut heartbeat).await;
                }
                _ = heartbeat.tick() => {
                    let actions = self.node.handle(Event::HeartbeatTimeout);
                    self.execute_actions(actions, &mut election_timeout, &mut heartbeat).await;
                }
            }
        }
    }

    fn input_to_event(&mut self, input: RunnerInput) -> Event {
        match input {
            RunnerInput::NetworkEvent(event) => event,
            RunnerInput::Propose { data, reply } => {
                let propose_id = self.next_propose_id;
                self.next_propose_id += 1;
                self.pending_proposes.insert(propose_id, reply);
                Event::Propose { data, propose_id }
            }
        }
    }

    async fn execute_actions(
        &mut self,
        actions: Vec<Action>,
        election_timeout: &mut std::pin::Pin<&mut tokio::time::Sleep>,
        heartbeat: &mut Interval,
    ) {
        // First pass: persistence
        for action in &actions {
            match action {
                Action::PersistQuorumState => {
                    self.node.quorum_state().persist(&self.quorum_state_path);
                }
                Action::AppendToLog(entry) => {
                    self.log.append(entry.clone()).await;
                }
                Action::TruncateLog(offset) => {
                    self.log.truncate(*offset).await;
                }
                _ => {}
            }
        }

        // Second pass: network, timers, notifications
        for action in actions {
            match action {
                Action::PersistQuorumState | Action::AppendToLog(_) | Action::TruncateLog(_) => {}

                Action::SendVote(to, req) => {
                    self.transport.send(to, RaftMessage::Vote(req)).await;
                }
                Action::SendVoteResponse(to, resp) => {
                    self.transport
                        .send(to, RaftMessage::VoteResponse(resp))
                        .await;
                }
                Action::SendBeginQuorumEpoch(targets, req) => {
                    for to in targets {
                        self.transport
                            .send(to, RaftMessage::BeginQuorumEpoch(req.clone()))
                            .await;
                    }
                }
                Action::SendEndQuorumEpoch(targets, req) => {
                    for to in targets {
                        self.transport
                            .send(to, RaftMessage::EndQuorumEpoch(req.clone()))
                            .await;
                    }
                }
                Action::SendFetchResponse(to, resp) => {
                    self.transport
                        .send(to, RaftMessage::FetchResponse(resp))
                        .await;
                }
                Action::SendFetchRequest(to, req) => {
                    self.transport
                        .send(to, RaftMessage::FetchRequest(req))
                        .await;
                }

                Action::ResetElectionTimer => {
                    election_timeout
                        .as_mut()
                        .reset(time::Instant::now() + self.random_election_duration());
                }
                Action::ResetHeartbeatTimer => {
                    heartbeat.reset();
                }

                Action::AdvanceHighWatermark(new_hwm) => {
                    self.last_applied_hwm = new_hwm;
                    // committed entries don't need to update MetadataImage, because Controller brokers don't keep a
                    // ready one. Instead, only the observer needs to it serve to clients.
                }

                Action::CommitPropose(propose_id) => {
                    if let Some(reply) = self.pending_proposes.remove(&propose_id) {
                        let _ = reply.send(Ok(()));
                    }
                }
                Action::RejectPropose(propose_id) => {
                    if let Some(reply) = self.pending_proposes.remove(&propose_id) {
                        let _ = reply.send(Err(ProposeError::NotLeader));
                    }
                }
            }
        }
    }

    fn random_election_duration(&self) -> Duration {
        let (min, max) = self.election_timeout_range;
        let ms = min + (self.next_propose_id % (max - min + 1));
        Duration::from_millis(ms)
    }
}
