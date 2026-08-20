# Raft Crate Architecture Plan

## Crate Properties

- **Dependencies:** serde only
- **No:** tokio, async, kafka types, channels
- **Purpose:** Pure Raft/KRaft state machine algorithm

## Core API

```rust
pub struct Node<L: RaftLog> { ... }

impl<L: RaftLog> Node<L> {
    pub fn new(id: u16, voters: Vec<u16>, quorum_state: QuorumState, log: L) -> Self
    pub fn handle(&mut self, event: Event) -> Vec<Action>
    pub fn quorum_state(&self) -> &QuorumState
}
```

- `handle()` is synchronous, pure-ish (reads log, mutates self, returns side effects)
- Always starts as Follower
- Node is in-memory authority — crash recovery: reload QuorumState from disk + re-read log

## File Layout

```
src/
  lib.rs           — pub mod, re-exports
  node.rs          — Node struct, handle() dispatch
  event.rs         — Event enum
  action.rs        — Action enum
  log.rs           — RaftLog trait, LogEntry, VecLog (#[cfg(test)])
  state.rs         — QuorumState (serde), Role enum
  rpc.rs           — VoteRequest/Response, FetchRequest/Response, Diverging, etc.
  election.rs      — impl Node: vote handling, election timeout, BeginQuorumEpoch
  replication.rs   — impl Node: fetch handling, HWM advance, divergence detection
```

## Types

### LogEntry (raft-owned, agnostic)

```rust
pub struct LogEntry {
    pub epoch: u32,
    pub offset: u64,
    pub data: Vec<u8>,
}
```

### RaftLog Trait (read-only, sync)

```rust
pub trait RaftLog {
    fn log_end_offset(&self) -> u64;
    fn epoch_at(&self, offset: u64) -> Option<u32>;
    fn last_epoch(&self) -> u32;
    fn entries(&self, start: u64, end: u64) -> Vec<LogEntry>;
    fn find_epoch_start(&self, epoch: u32) -> u64;
}
```

Implementor must guarantee read-after-write consistency.

### Event Enum

```rust
pub enum Event {
    ElectionTimeout,
    HeartbeatTimeout,
    VoteRequest { from: u16, req: VoteRequest },
    VoteResponse { from: u16, resp: VoteResponse },
    BeginQuorumEpoch { from: u16, req: BeginQuorumEpochRequest },
    EndQuorumEpoch { from: u16, req: EndQuorumEpochRequest },
    FetchRequest { from: u16, req: FetchRequest },
    FetchResponse { from: u16, resp: FetchResponse },
    Propose { data: Vec<u8>, propose_id: u64 },
}
```

### Action Enum

```rust
pub enum Action {
    // Persistence (execute first, fsync)
    PersistQuorumState,
    AppendToLog(LogEntry),
    TruncateLog(u64),

    // Network (execute after persists)
    SendVote(u16, VoteRequest),
    SendVoteResponse(u16, VoteResponse),
    SendBeginQuorumEpoch(Vec<u16>, BeginQuorumEpochRequest),
    SendEndQuorumEpoch(Vec<u16>, EndQuorumEpochRequest),
    SendFetchResponse(u16, FetchResponse),
    SendFetchRequest(u16, FetchRequest),

    // Timers
    ResetElectionTimer,
    ResetHeartbeatTimer,

    // Client notifications (Runner routes by propose_id)
    CommitPropose(u64),
    RejectPropose(u64),
}
```

### QuorumState

```rust
#[derive(Serialize, Deserialize)]
pub struct QuorumState {
    pub current_epoch: u32,
    pub voted_for: Option<u16>,
}
```

### Node Fields

```rust
pub struct Node<L: RaftLog> {
    id: u16,
    voters: Vec<u16>,
    log: L,

    // Election state
    current_epoch: u32,
    voted_for: Option<u16>,
    role: Role,
    leader_id: Option<u16>,
    votes_received: HashSet<u16>,

    // Leader state
    voter_fetch_offsets: HashMap<u16, u64>,
    high_watermark: u64,

    // Propose tracking
    pending_proposes: Vec<(u64, u64)>, // (log_offset, propose_id)
}
```

## Runner Contract (lives outside this crate)

Runner is the async executor that:
1. Owns the network (receives RPCs, feeds Events to Node)
2. Owns timers (election timeout, heartbeat) — fires timeout Events
3. Executes Actions returned by `node.handle()`
4. **Ordering rule:** always persist before send (PersistQuorumState/AppendToLog before any Send*)
5. Buffers propose channels: maps `propose_id -> oneshot::Sender`
6. On `CommitPropose(id)` / `RejectPropose(id)` — routes to correct channel
7. Reads `node.quorum_state()` when executing `PersistQuorumState` — fsync + atomic rename

## Testing Strategy

- **Unit tests:** VecLog (in-memory, `#[cfg(test)]` in log.rs). Construct Node, push Event, assert Actions + state.
- **Integration tests:** Real storage (PartitionHandle with tempdir).
- **Simple tests:** Direct `Node::new(...)` construction.
- **Complex scenarios:** Builder pattern for multi-step (full election, log divergence).

## Design Principles

| Principle | Implementation |
|-----------|---------------|
| Agnostic | Log trait defined here, implemented externally on PartitionHandle |
| Channel-free | propose_ids (u64) instead of oneshot senders |
| No synthetic events | Node re-reads log state via trait after Runner writes |
| Flat actions | One variant per thing Runner does — no nesting |
| Testable | Pure state machine, mock log, assert actions |
| No Clock/Cluster traits | Runner owns timers/randomness, voters is Vec<u16> |
