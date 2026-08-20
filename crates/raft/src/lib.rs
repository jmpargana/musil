pub mod election;
pub mod event;
pub mod log;
pub mod node;
pub mod quorum;
pub mod quorum_state;
pub mod replication;
pub mod rpc;
pub mod state;

// use std::time::Duration;

// /// Adaption of actual Raft consensus, as kafka relies on pull-based consensus,
// /// rather than pushing `ReplicateLog` (`LogRequest`/`LogResponse`) as traditional raft algorithm.
// use tokio::{
//     sync::{mpsc, oneshot},
//     time,
// };

// mod refactor;

// #[async_trait]
// trait Log {}

// // TODO: potential additional traits:
// // - replicate
// // - append

// struct RequestVote {
//     term: u64,
//     candidate_id: u16,
//     last_log_index: u64,
//     last_log_term: u64,
// }

// struct RequestVoteResponse {
//     term: u64,
//     vote_granted: bool,
// }

// struct Node<L: Log> {
//     id: u16,

//     // disk vars
//     current_term: u64,
//     voted_for: Option<u16>,
//     commit_length: u64,
//     log: L,

//     // tmp
//     current_role: Role,
//     current_leader: Option<u16>,
//     votes_received: Vec<u16>,

//     // These are like high watermark for each replica. Instead of u16 representing node_id
//     // we can use a trait for the ID.
//     sent_length: HashMap<u16, u64>,
//     acked_length: HashMap<u16, u64>,

//     // manager
//     rx: mpsc::Receiver<Command>,
//     timeout_timer: time::Duration,
// }

// enum Command {
//     VoteRequest {
//         req: RequestVote,
//         done: oneshot::Sender<RequestVoteResponse>,
//     },
//     VoteResponse {
//         voter_id: i32,
//         term: u32,
//         granted: bool,
//     },

//     // In KRaft ReplicateLog is actually pull-based, so it's different.
//     LogRequest {
//         leader_id: i32,
//         current_term: u32,
//         prefix_len: u64,
//         prefix_term: u64,
//         commit_length: u32,
//         suffix: Vec<String>, // FIXME: Actually should be log slice
//     },
//     LogResponse {
//         node_id: i32,
//         current_term: u32,
//         ack: Ack,
//         something: bool,
//     },
// }

// enum Role {
//     Follower,
//     Candidate,
//     Leader,
// }

// // Aka. log end offset - high watermark
// enum Ack {}

// enum ElectionOutcome {
//     Won,     // Votes from majority including self
//     Lost,    // AppendEntries with term >= self
//     Timeout, // No resolution
// }

// impl<L> Node<L>
// where
//     L: Log,
// {
//     // TODO: pass init for log
//     fn init(rx: mpsc::Receiver<Command>) -> Self {
//         let current_term = 0;
//         let voted_for = None;
//         let commit_length = 0;

//         // recover from crash to override variables.

//         Self {
//             // These are loaded from disk
//             current_term,
//             voted_for,
//             commit_length,
//             log: (),
//             current_role: Role::Follower,
//             current_leader: None,
//             votes_received: vec![],
//             sent_length: HashMap::new(),
//             acked_length: HashMap::new(),
//             rx,
//         }
//     }

//     fn reset_timer(&mut self) {}

//     async fn init_election(&mut self) {
//         self.current_term += 1;
//         self.voted_for = Some(self.id);
//         self.current_role = Role::Candidate;
//         // persist term and voted to disk
//         self.run_election().await;
//     }

//     async fn run_election(&mut self) -> ElectionOutcome {
//         let mut votes_received = 1;
//         let majority = (self.peers.len() + 1) / 2 + 1;
//         let timeout = self.random_election_timeout();

//         // TODO: refactor with clean arch
//         let mut pending = send_vote_requests(&self.peers, &self.request_vote());

//         loop {
//             tokio::select! {
//                 Some(res) = pending.next() => {
//                     if res.term > self.current_term {
//                         self.become_follower(res.term);
//                         return ElectionOutcome::Lost;
//                     }

//                     if res.vote_granted {
//                         votes_received += 1;
//                         if votes_received >= majority {
//                             return ElectionOutcome::Won;
//                         }
//                     }
//                 }

//                 Some(leader_msg) = self.incoming.recv() => {
//                     if leader_msg.term >= self.current_term {
//                         self.become_follower(leader_msg.term);
//                         return ElectionOutcome::Lost;
//                     }
//                 }

//                 _ = tokio::time::sleep(timeout) => {
//                     return ElectionOutcome::Timeout;
//                 }
//             }
//         }
//     }

//     fn reset_election_timer(&mut self) {
//         // TODO: need both a timer and a duration. Investigation needed.
//     }

//     /// Conditions for the range:
//     /// - Lower bound > broadcast time — a leader must be able to heartbeat all nodes before any timeout fires
//     /// - Upper bound < MTBF — must elect before another node fails
//     /// - Spread (upper - lower) — wide enough that simultaneous timeouts are rare
//     /// In practice, the range should be roughly [T, 2T] where T is 5-10x the network RTT.
//     fn random_election_timeout() -> Duration {
//         let ms = rand::rng().random_range(150..=300);
//         Duration::from_millis(ms)
//     }

//     fn can_grant_vote(&mut self, req: RequestVote) -> bool {
//         let term_check = req.term >= self.current_term;
//         let vote_availability = self.voted_for.is_none();

//         /// Only node with all committed entries can win an election.
//         /// A.last_term > B.last_term
//         /// OR A.last_term == B.last_term AND A.last_index >= B.last_index
//         let log_comparison = if req.last_log_term != self.log.last_term() {
//             req.last_log_term > self.log.last_term()
//         } else {
//             req.last_log_index >= self.log.last_index()
//         };

//         term_check && vote_availability && log_comparison
//     }

//     fn update_term(&mut self, term: u64) {
//         self.current_term = term;
//         self.voted_for = None;
//         self.role = Role::Follower;
//         // TODO:
//         self.log.store();
//     }

//     async fn run(&mut self) {
//         loop {
//             tokio::select! {
//                 Some(cmd) = self.rx.recv() => {
//                     match cmd {
//                         Command::VoteRequest {
//                             req,
//                             done
//                         } => {
//                             // Always update term if sender is higher
//                             if req.term > self.current_term {
//                                 self.update_term(candidate_term);
//                             }

//                             if req.term < self.current_term {
//                                 done.send(RequestVoteResponse { term: self.current_term, vote_granted: false }).await;
//                                 continue;
//                             }

//                             let mut vote_granted = false;
//                             if self.can_grant_vote(req) {
//                                 self.vote_granted = true;
//                                 self.voted_for = Some(req.candidate_id);
//                                 // TODO:
//                                 self.log.store();
//                                 self.reset_election_timer();
//                             }

//                             done.send(RequestVoteResponse { term: self.current_term, vote_granted }).await;
//                         },
//                         Command::VoteResponse {
//                             voter_id,
//                             term,
//                             granted,
//                         } => todo!(),
//                         Command::LogRequest {
//                             leader_id,
//                             current_term,
//                             prefix_len,
//                             prefix_term,
//                             commit_length,
//                             suffix,
//                         } => todo!(),
//                         Command::LogResponse {
//                             node_id,
//                             current_term,
//                             ack,
//                             something,
//                         } => todo!(),
//                     }
//                 }

//                 _ = &mut self.timeout_timer => {

//                 }
//             }
//         }
//     }
// }
