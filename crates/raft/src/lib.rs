/// Adaption of actual Raft consensus, as kafka relies on pull-based consensus,
/// rather than pushing `ReplicateLog` (`LogRequest`/`LogResponse`) as traditional raft algorithm.
use tokio::sync::mpsc;

#[async_trait]
trait Log {}

// TODO: potential additional traits:
// - replicate
// - append

struct Node<L: Log> {
    // disk vars
    current_term: u32,
    voted_for: Option<i32>,
    commit_length: u64,
    log: L,

    // tmp
    current_role: Role,
    current_leader: Option<i32>,
    votes_received: Vec<i32>,

    // These are like high watermark for each replica. Instead of i32 representing node_id
    // we can use a trait for the ID.
    sent_length: HashMap<i32, u64>,
    acked_length: HashMap<i32, u64>,

    // manager
    // timeout
    rx: mpsc::Receiver<Command>,
}

enum Command {
    VoteRequest {
        candidate_id: i32,
        candidate_term: u32,
        candidate_log_length: u64, // log end offset
        candidate_log_term: u32,   // FIXME: what's the difference to term?
    },
    VoteResponse {
        voter_id: i32,
        term: u32,
        granted: bool,
    },

    // In KRaft ReplicateLog is actually pull-based, so it's different.
    LogRequest {
        leader_id: i32,
        current_term: u32,
        prefix_len: u64,
        prefix_term: u64,
        commit_length: u32,
        suffix: Vec<String>, // FIXME: Actually should be log slice
    },
    LogResponse {
        node_id: i32,
        current_term: u32,
        ack: Ack,
        something: bool,
    },
}

enum Role {
    Follower,
    Candidate,
    Leader,
}

// Aka. log end offset - high watermark
enum Ack {}

impl<L> Node<L>
where
    L: Log,
{
    // TODO: pass init for log
    fn init(rx: mpsc::Receiver<Command>) -> Self {
        let current_term = 0;
        let voted_for = None;
        let commit_length = 0;

        // recover from crash to override variables.

        Self {
            // These are loaded from disk
            current_term,
            voted_for,
            commit_length,
            log: (),
            current_role: Role::Follower,
            current_leader: None,
            votes_received: vec![],
            sent_length: HashMap::new(),
            acked_length: HashMap::new(),
            rx,
        }
    }

    async fn run(&mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                Command::VoteRequest {
                    candidate_id,
                    candidate_term,
                    candidate_log_length,
                    candidate_log_term,
                } => todo!(),
                Command::VoteResponse {
                    voter_id,
                    term,
                    granted,
                } => todo!(),
                Command::LogRequest {
                    leader_id,
                    current_term,
                    prefix_len,
                    prefix_term,
                    commit_length,
                    suffix,
                } => todo!(),
                Command::LogResponse {
                    node_id,
                    current_term,
                    ack,
                    something,
                } => todo!(),
            }
        }
    }
}
