# Turmoil Raft Implementation Design

Low-level specification for implementing Raft (+ KV + Sharding) in Rust with
Tonic gRPC and Turmoil deterministic simulation. Phases map to MIT 6.5840 labs.

**Implementation order:**

| # | Phase | 6.5840 Lab | Difficulty |
|---|-------|-----------|------------|
| 0 | Networking & Boilerplate | -- | DONE |
| 1 | Leader Election | 3A | Moderate |
| 2 | Log Replication | 3B | Hard |
| 3 | Persistence | 3C | Hard |
| 4 | Log Compaction / Snapshots | 3D | Hard |
| 5 | Fault-tolerant KV Service | 4A-4C | Hard |
| 6 | Sharded KV | 5A-5D | Very Hard |

**Critical reference:** Raft extended paper, **Figure 2**. Follow it religiously.
Every `if` check, every field update, every invariant -- implement them all.

---

## Phase 0: Networking & Boilerplate -- DONE

Tonic gRPC works inside Turmoil's simulated network. The bridge lives in
`tests/common/mod.rs`.

## Phase 1: Leader Election

**Goal:** Nodes elect exactly one leader per term. Leader sends heartbeats.
Followers that don't hear from a leader start elections. New leader elected
within ~2 seconds of old leader failure.

### File: `src/raft/core.rs`

**Replace the `Raft` struct with full Figure 2 state:**

```rust
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Role { Follower, Candidate, Leader }

pub struct Raft {
    // -- Identity --
    id: u64,
    peers: BTreeMap<u64, RaftClient>,

    // -- Persistent state (Figure 2) --
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<LogEntry>,          // 1-indexed: log[0] is a dummy sentinel

    // -- Volatile state (all servers) --
    commit_index: u64,
    last_applied: u64,
    role: Role,

    // -- Volatile state (leader only, reinit on election) --
    next_index: BTreeMap<u64, u64>,   // peer_id -> next log index to send
    match_index: BTreeMap<u64, u64>,  // peer_id -> highest replicated index

    // -- Channels --
    rx: mpsc::Receiver<RaftMsg>,
    apply_tx: mpsc::UnboundedSender<ApplyMsg>, // sends committed entries to state machine

    // -- Timers --
    election_deadline: tokio::time::Instant,  // randomized; reset on heartbeat/vote grant
    heartbeat_interval: Duration,             // fixed ~100ms
}
```

**Key constants:**
```rust
const ELECTION_TIMEOUT_MIN_MS: u64 = 300;
const ELECTION_TIMEOUT_MAX_MS: u64 = 500;
const HEARTBEAT_INTERVAL_MS: u64 = 100;  // must be << election timeout
```

**`ApplyMsg` enum** (used by all later phases):
```rust
pub enum ApplyMsg {
    Command { index: u64, command: Vec<u8> },
    Snapshot { index: u64, term: u64, data: Vec<u8> },  // Phase 4
}
```

**`RaftMsg` enum -- add variants for client commands:**
```rust
pub enum RaftMsg {
    AppendEntries {
        req: AppendEntriesRequest,
        reply: oneshot::Sender<AppendEntriesResponse>,
    },
    RequestVote {
        req: RequestVoteRequest,
        reply: oneshot::Sender<RequestVoteResponse>,
    },
    InstallSnapshot {
        req: InstallSnapshotRequest,
        reply: oneshot::Sender<InstallSnapshotResponse>,
    },
    // Phase 2: client submits a command
    Start {
        command: Vec<u8>,
        reply: oneshot::Sender<StartResult>,
    },
    // Phase 2: query current state
    GetState {
        reply: oneshot::Sender<(u64, bool)>,  // (term, is_leader)
    },
}
```

**Public API** (called from outside via a handle):
```rust
/// RaftHandle is a cheap clone-able handle to send messages to the core loop.
#[derive(Clone)]
pub struct RaftHandle {
    tx: mpsc::Sender<RaftMsg>,
}

impl RaftHandle {
    /// Submit a command. Returns (index, term, is_leader).
    /// Only the leader accepts commands; others return is_leader=false.
    pub async fn start(&self, command: Vec<u8>) -> Result<StartResult, ()>;

    /// Returns (current_term, is_leader).
    pub async fn get_state(&self) -> Result<(u64, bool), ()>;
}
```

**Main loop (`Raft::run`) -- rewrite with randomized election timer:**
```rust
pub async fn run(mut self) {
    let mut heartbeat_interval = tokio::time::interval(self.heartbeat_interval);
    loop {
        tokio::select! { biased;
            // 1. Process incoming RPCs and client requests (highest priority)
            Some(msg) = self.rx.recv() => {
                self.handle_msg(msg).await;
            },
            // 2. Election timeout fires
            _ = tokio::time::sleep_until(self.election_deadline) => {
                self.start_election().await;
            },
            // 3. Heartbeat timer (leader only sends; followers ignore)
            _ = heartbeat_interval.tick() => {
                if self.role == Role::Leader {
                    self.send_heartbeats().await;
                }
            },
        }
        // After every iteration: apply committed entries
        self.apply_committed();
    }
}
```

**`reset_election_timer()`:**
- Set `election_deadline = Instant::now() + random(300..500)ms`.
- Called on: startup, receiving valid AppendEntries, granting a vote.

**`start_election()`:**
1. `self.role = Candidate`
2. `self.current_term += 1`
3. `self.voted_for = Some(self.id)`
4. Reset election timer (in case election stalls)
5. `votes_received = 1` (self-vote)
6. Send `RequestVote` to all peers in parallel (spawn tasks)
7. Collect responses: if `vote_granted` and term matches, increment votes.
   If `votes_received > peers.len() / 2`, become leader.

**`become_leader()`:**
- `self.role = Leader`
- Initialize `next_index[peer] = last_log_index + 1` for all peers
- Initialize `match_index[peer] = 0` for all peers
- Immediately send heartbeats (empty AppendEntries) to assert authority

**`handle_request_vote(req)` -- implement Figure 2 RequestVote RPC receiver:**
1. If `req.term < self.current_term` -> reply `(current_term, false)`
2. If `req.term > self.current_term` -> step down: `become_follower(req.term)`
3. If `voted_for` is None or == `req.candidate_id`:
   - **Election restriction (Section 5.4.1):** grant vote ONLY if candidate's
     log is at least as up-to-date as ours:
     - `req.last_log_term > our_last_term`, OR
     - `req.last_log_term == our_last_term && req.last_log_index >= our_last_index`
   - If granted: `voted_for = Some(req.candidate_id)`, reset election timer
4. Reply `(current_term, vote_granted)`

**`handle_append_entries(req)` -- implement Figure 2 AppendEntries RPC receiver:**
1. If `req.term < self.current_term` -> reply `(current_term, false)`
2. Reset election timer (we heard from a valid leader)
3. If `req.term > self.current_term` -> `become_follower(req.term)`
4. If `self.role == Candidate` -> `become_follower(req.term)` (leader established)
5. **For Phase 1 (heartbeats only):** reply `(current_term, true)`.
   Log consistency check and log manipulation are added in Phase 2.

**`become_follower(term)`:**
- `self.role = Follower`
- `self.current_term = term`
- `self.voted_for = None`

**`step_down_if_stale(remote_term) -> bool`:**
- Common helper: if `remote_term > self.current_term`, become follower, return true.
- Called at the top of every RPC handler AND when processing RPC responses.

**`apply_committed()`:**
- While `last_applied < commit_index`:
  - `last_applied += 1`
  - Send `ApplyMsg::Command { index, command }` on `apply_tx`

### File: `src/raft/rpc.rs`

**Implement the `request_vote` handler** (currently `unimplemented!()`):
- Same pattern as `append_entries`: create oneshot, wrap in `RaftMsg::RequestVote`,
  send to core, await response.

**Leave `install_snapshot` as `unimplemented!()` until Phase 4.**

### File: `src/raft/log.rs`

**Implement log helper methods:**
```rust
use crate::pb::raft::LogEntry;

/// The log is 1-indexed. Index 0 is a dummy sentinel with term=0.
/// self.log[0] = sentinel, self.log[1] = first real entry, etc.

pub fn last_log_index(log: &[LogEntry]) -> u64;  // log.len() - 1
pub fn last_log_term(log: &[LogEntry]) -> u64;   // log[last].term
pub fn term_at(log: &[LogEntry], index: u64) -> Option<u64>;
```

### File: `tests/raft_election.rs`

**Test `test_initial_election`:**
1. Create 3-node cluster (same setup as ping_test, but with real election logic).
2. Step simulation for ~2 seconds.
3. Query all nodes via `RaftHandle::get_state()`.
4. Assert exactly ONE node reports `is_leader=true`.
5. Assert all nodes agree on the same term.

**Test `test_reelection`:**
1. Create 5-node cluster. Wait for leader.
2. Partition the leader (turmoil: `sim.partition("server-X", "server-Y")`).
3. Step simulation. Assert new leader elected among remaining nodes.
4. Heal partition. Step simulation.
5. Assert old leader stepped down (is now follower with higher term).
6. Assert exactly one leader in the cluster.

**Test `test_many_elections`:**
1. Create 7-node cluster.
2. Repeatedly: pick random nodes to partition, wait for election, heal, repeat.
3. After each round: assert exactly one leader, all nodes agree on term.

**How to run:** `cargo test --test raft_election`

---

## Phase 2: Log Replication

**Goal:** Leader accepts commands via `start()`, replicates log entries to
followers, advances `commit_index` when majority has replicated, and delivers
committed entries on `apply_tx`.

### File: `src/raft/core.rs`

**Add `handle_msg` arm for `RaftMsg::Start`:**
```rust
RaftMsg::Start { command, reply } => {
    if self.role != Role::Leader {
        let _ = reply.send(StartResult { index: 0, term: 0, is_leader: false });
        return;
    }
    let index = last_log_index(&self.log) + 1;
    self.log.push(LogEntry { index, term: self.current_term, command });
    // Trigger immediate replication (don't wait for next heartbeat)
    self.send_append_entries_to_all().await;
    let _ = reply.send(StartResult { index, term: self.current_term, is_leader: true });
}
```

**Expand `send_heartbeats()` into `send_append_entries_to_all()`:**
For each peer:
1. `prev_log_index = next_index[peer] - 1`
2. `prev_log_term = log[prev_log_index].term`
3. `entries = log[next_index[peer]..]` (everything the peer hasn't seen)
4. Send `AppendEntriesRequest { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit: commit_index }`
5. Spawn task to send and handle response.

**Handle AppendEntries responses (in spawned task, send result back to core):**

Add a new `RaftMsg` variant:
```rust
RaftMsg::AppendEntriesReply {
    peer_id: u64,
    req_prev_log_index: u64,  // what we sent, for context
    entries_len: u64,          // how many entries we sent
    resp: AppendEntriesResponse,
}
```

Processing:
- If `resp.success`:
  - `match_index[peer] = req_prev_log_index + entries_len`
  - `next_index[peer] = match_index[peer] + 1`
  - Call `maybe_advance_commit_index()`
- If `!resp.success`:
  - If `resp.term > current_term` -> step down
  - Else: decrement `next_index[peer]` (simple backtracking for now;
    fast backup optimization added in Phase 3)

**`maybe_advance_commit_index()`:**
- For each index N from `commit_index+1` to `last_log_index`:
  - Count peers where `match_index[peer] >= N` (include self)
  - If count > (total_nodes / 2) AND `log[N].term == current_term`:
    - `commit_index = N`
- **Important:** only commit entries from the current term (Figure 8 safety).

**Expand `handle_append_entries(req)` -- full log consistency check:**
1. (Same term checks as Phase 1)
2. **Consistency check:** if `prev_log_index > 0`:
   - If our log doesn't have index `prev_log_index` -> reply false with
     `conflict_index = len(log)`, `conflict_term = 0`
   - If `log[prev_log_index].term != prev_log_term` -> reply false with
     `conflict_term = log[prev_log_index].term`,
     `conflict_index = first index where term == conflict_term`
3. **Append entries:** for each entry in `req.entries`:
   - If we have a conflicting entry at that index (different term), truncate
     our log from that point onward
   - Append the entry
4. **Update commit_index:** `commit_index = min(req.leader_commit, index of last new entry)`
5. Reply `(current_term, true)`

### File: `src/raft/log.rs`

**Add:**
```rust
/// Find first index with the given term (for conflict resolution).
pub fn first_index_for_term(log: &[LogEntry], term: u64) -> u64;
```

### File: `tests/raft_replication.rs`

**Test `test_basic_agree`:**
1. 3-node cluster, wait for leader.
2. Submit 3 commands via `start()` on the leader.
3. Step simulation.
4. Assert all 3 nodes have the same log entries.
5. Assert `apply_tx` received all 3 commands on all nodes, in order.

**Test `test_follower_failure`:**
1. 3-node cluster, wait for leader.
2. Submit a command. Verify all agree.
3. Partition one follower.
4. Submit another command. Verify the 2 remaining nodes agree.
5. Heal partition. Step simulation.
6. Verify the reconnected follower catches up.

**Test `test_leader_failure`:**
1. 5-node cluster, wait for leader.
2. Submit a command. Verify all agree.
3. Partition the leader.
4. Submit a command to the new leader.
5. Heal partition.
6. Verify the old leader's log is reconciled (conflicting entries truncated).

**Test `test_no_agree_without_majority`:**
1. 5-node cluster, wait for leader.
2. Partition so leader only has 1 follower (minority).
3. Submit command to leader. Step simulation.
4. Verify command is NOT committed (not enough replicas).
5. Heal partition. Verify command eventually commits.

**Test `test_concurrent_starts`:**
1. 3-node cluster, wait for leader.
2. Submit 5 commands concurrently (all at once).
3. Verify all 5 are committed, in some consistent order, on all nodes.

**How to run:** `cargo test --test raft_replication`

---

## Phase 3: Persistence

**Goal:** Raft nodes survive crashes. After `turmoil::sim.crash(node)` +
`sim.bounce(node)`, the node restores its state and rejoins the cluster.

### Important: Turmoil does NOT mock the filesystem

We need an in-memory `Persister` abstraction, shared between the Raft core
and the test harness via `Arc<Mutex<_>>`. Turmoil preserves host-local `Arc`
state across crash/bounce if the host closure re-reads from it.

### File: `src/raft/persist.rs` (NEW)

```rust
use serde::{Serialize, Deserialize};
use crate::pb::raft::LogEntry;

#[derive(Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>,  // LogEntry needs Serialize/Deserialize
}

/// In-memory persistence that survives simulated crashes.
/// Shared via Arc<Mutex<Persister>> between test harness and Raft.
pub struct Persister {
    raft_state: Vec<u8>,
    snapshot: Vec<u8>,       // Phase 4
}

impl Persister {
    pub fn new() -> Self;
    pub fn save_raft_state(&mut self, data: Vec<u8>);
    pub fn read_raft_state(&self) -> Vec<u8>;
    pub fn save_snapshot(&mut self, data: Vec<u8>);    // Phase 4
    pub fn read_snapshot(&self) -> Vec<u8>;             // Phase 4
    pub fn raft_state_size(&self) -> usize;             // Phase 4
}
```

### File: `src/raft/core.rs`

**Add `persister: Arc<Mutex<Persister>>` field to `Raft`.**

**`persist()` method:**
- Serialize `PersistentState { current_term, voted_for, log }` with `bincode`.
- Call `persister.lock().save_raft_state(bytes)`.
- **Called after every mutation to `current_term`, `voted_for`, or `log`.**

**`restore()` method:**
- Called in `Raft::new()` before entering the main loop.
- Read bytes from `persister.lock().read_raft_state()`.
- If non-empty, deserialize and populate fields.

**Add fast log backup optimization to AppendEntries rejection handling:**

When leader receives a failed AppendEntries response:
- **Case 1:** `conflict_term == 0` (follower's log is too short):
  `next_index[peer] = conflict_index`
- **Case 2:** `conflict_term > 0`, leader HAS entries with `conflict_term`:
  `next_index[peer] = last_index_of(conflict_term) + 1`
- **Case 3:** `conflict_term > 0`, leader does NOT have `conflict_term`:
  `next_index[peer] = conflict_index`

This replaces the simple `next_index -= 1` from Phase 2.

### File: `src/raft/mod.rs`

Add `pub mod persist;`

### File: `Cargo.toml`

Add `bincode = "1"` to dependencies (or `serde_json` if you prefer readability
during debugging).

### File: `tests/raft_persistence.rs`

**Test setup requires a different pattern** -- the `Persister` must be created
outside the host closure so it survives crash/bounce:

```rust
let persister = Arc::new(Mutex::new(Persister::new()));
let p = persister.clone();
sim.host("server-1", move || {
    let p = p.clone();
    async move {
        // Raft::new reads from persister on startup
        let raft = Raft::new(1, rx, peers, p);
        raft.run().await;
    }
});
```

**Test `test_persist_basic`:**
1. 3-node cluster with persisters.
2. Elect leader, submit a command, verify committed.
3. Crash all 3 nodes (`sim.crash()`), then bounce all (`sim.bounce()`).
4. Wait for new election.
5. Verify the committed command is still in all logs.

**Test `test_persist_more`:**
1. 5-node cluster. Submit several commands.
2. Crash 2 followers, bounce them. Verify they catch up.
3. Crash the leader, bounce it. Verify new leader elected, old leader rejoins.
4. Submit more commands. Verify everything committed on all nodes.

**Test `test_figure8`:**
Reproduce the Figure 8 scenario from the Raft paper:
1. 5-node cluster. Leader commits entry in term 2.
2. Partition leader. New leader in term 3 commits different entry at same index.
3. Heal. Verify the term-2 entry is overwritten by the term-3 entry.
4. This tests that leaders only commit entries from their current term.

**Test `test_unreliable_agree`:**
1. 5-node cluster with `sim.fail_rate(0.1)` (10% message loss).
2. Submit 50 commands in a loop.
3. Verify all eventually committed on all nodes.

**How to run:** `cargo test --test raft_persistence`

---

## Phase 4: Log Compaction / Snapshots

**Goal:** State machine can take a snapshot, discarding old log entries.
Leader sends snapshots to slow followers via InstallSnapshot RPC.

### File: `src/raft/core.rs`

**Add snapshot state to `Raft`:**
```rust
// Snapshot metadata (replaces discarded log prefix)
snapshot_last_index: u64,  // last index included in snapshot
snapshot_last_term: u64,   // term of that index
```

**Modify log indexing:**
After snapshotting, `self.log` no longer starts at index 0. The sentinel at
`log[0]` has `index = snapshot_last_index, term = snapshot_last_term`.
All log access must go through helpers that translate global index to vec index:
```rust
fn vec_index(&self, global_index: u64) -> usize {
    (global_index - self.snapshot_last_index) as usize
}
fn global_index(&self, vec_index: usize) -> u64 {
    vec_index as u64 + self.snapshot_last_index
}
```

**`snapshot(index, data)` method** -- called by state machine (KV server):
1. If `index <= snapshot_last_index`, ignore (already snapshotted past this).
2. Trim `self.log`: keep only entries after `index`.
3. Insert new sentinel: `LogEntry { index, term: log[index].term, command: vec![] }`.
4. `snapshot_last_index = index`, `snapshot_last_term = term`.
5. Persist both raft state AND snapshot via persister.

**`handle_install_snapshot(req)`:**
1. If `req.term < current_term` -> reply with current term.
2. Reset election timer.
3. If `req.last_included_index <= snapshot_last_index` -> reply (already have it).
4. Trim log: discard everything up to `req.last_included_index`.
5. Update `snapshot_last_index`, `snapshot_last_term`.
6. Save snapshot in persister.
7. Send `ApplyMsg::Snapshot { ... }` on `apply_tx` so the state machine restores.

**Leader: send InstallSnapshot instead of AppendEntries when `next_index[peer] <= snapshot_last_index`:**
- The leader no longer has the entries the follower needs.
- Send the full snapshot instead.

**Add `RaftMsg::Snapshot` variant** (for state machine to request trimming):
```rust
RaftMsg::Snapshot { index: u64, data: Vec<u8> }
```

### File: `src/raft/rpc.rs`

**Implement `install_snapshot`** (currently `unimplemented!()`):
Same oneshot+mpsc pattern as the other RPCs.

### File: `src/raft/log.rs`

**Update all helpers** to account for `snapshot_last_index` offset.

### File: `tests/raft_snapshot.rs`

**Test `test_snapshot_basic`:**
1. 3-node cluster. Submit 100 commands.
2. Trigger snapshot at index 50 on all nodes (simulating state machine snapshot).
3. Verify logs are trimmed on all nodes.
4. Submit 50 more commands. Verify all committed.

**Test `test_snapshot_install`:**
1. 3-node cluster. Submit 50 commands.
2. Partition a follower.
3. Submit 100 more commands. Trigger snapshot at index 100 on leader+other follower.
4. Heal partition. The lagging follower cannot catch up via AppendEntries
   (leader discarded those entries). Leader must send InstallSnapshot.
5. Verify follower catches up and all nodes agree.

**Test `test_snapshot_crash`:**
1. 3-node cluster. Submit commands. Snapshot.
2. Crash all nodes. Bounce.
3. Verify snapshot + remaining log entries are intact.
4. Submit more commands. Verify committed.

**How to run:** `cargo test --test raft_snapshot`

---

## Phase 5: Fault-tolerant KV Service

**Goal:** A linearizable key-value store replicated across a Raft cluster.
Clients see a single, consistent system even when a minority of servers fail.

This phase has 3 sub-parts:
- **5A:** RSM (Replicated State Machine) layer
- **5B:** KV service without snapshots
- **5C:** KV service with snapshots

### File: `src/rsm.rs` (NEW) -- Phase 5A

The RSM layer sits between the service (KV) and Raft. It handles:
- Submitting operations to Raft
- Waiting for them to be committed and applied
- Detecting leadership changes
- Deduplicating client requests

```rust
/// Generic replicated state machine layer.
pub struct Rsm<S: StateMachine> {
    raft: RaftHandle,
    apply_rx: mpsc::UnboundedReceiver<ApplyMsg>,
    state_machine: S,
    // Pending requests: index -> oneshot sender for the result
    pending: HashMap<u64, oneshot::Sender<Option<Vec<u8>>>>,
    // Term when each pending request was submitted (to detect leader changes)
    pending_terms: HashMap<u64, u64>,
}

pub trait StateMachine: Send + 'static {
    /// Apply an operation, return the result.
    fn apply(&mut self, command: &[u8]) -> Vec<u8>;
    /// Take a snapshot of current state.
    fn snapshot(&self) -> Vec<u8>;
    /// Restore from a snapshot.
    fn restore(&mut self, data: &[u8]);
}
```

**`Rsm::submit(command)` method:**
1. Call `raft.start(command)`.
2. If not leader, return `Err(WrongLeader)`.
3. Record `(index, term)` and a oneshot channel in `pending`.
4. Await the oneshot. If the committed entry at that index doesn't match
   (different term or different command), return `Err(WrongLeader)` --
   leadership changed and a different command was committed at that index.

**`Rsm::run()` loop:**
- Read from `apply_rx`.
- For each `ApplyMsg::Command { index, command }`:
  - Call `state_machine.apply(&command)` to get result.
  - If `pending` has a waiter for this index AND the term matches:
    send the result through the oneshot.
  - Otherwise: still apply (the state machine must see all committed ops)
    but nobody is waiting locally.
- For each `ApplyMsg::Snapshot { data, .. }`:
  - Call `state_machine.restore(&data)`.
  - Clear all pending requests (stale after snapshot).

### File: `src/kv/server.rs` -- Phase 5B

```rust
/// KV state machine implementing the StateMachine trait.
pub struct KvStateMachine {
    store: HashMap<String, String>,
    // Duplicate detection: client_id -> (last_seq_num, last_result)
    dup_table: HashMap<String, (u64, String)>,
}
```

**`StateMachine::apply(command)` implementation:**
1. Deserialize command into `KvOp` enum (Get/Put/Append with client_id + seq_num).
2. **Duplicate detection:** if `dup_table[client_id].seq_num >= req.seq_num`,
   return the cached result (already applied).
3. Execute the operation:
   - `Get(key)` -> return `store[key]` or empty string
   - `Put(key, value)` -> `store[key] = value`
   - `Append(key, value)` -> `store[key] += value`
4. Update `dup_table[client_id] = (seq_num, result)`.
5. Serialize and return the result.

**Important:** Every Get MUST go through Raft (no read-only optimization).
This ensures linearizability -- a stale leader cannot serve reads.

**`KvOp` enum (serialized as the Raft command bytes):**
```rust
#[derive(Serialize, Deserialize)]
pub enum KvOp {
    Get { key: String, client_id: String, seq_num: u64 },
    Put { key: String, value: String, client_id: String, seq_num: u64 },
    Append { key: String, value: String, client_id: String, seq_num: u64 },
}
```

**gRPC handlers (`impl Kv for KvServer`):**
- `get(req)` / `put_append(req)`:
  1. Serialize the operation into bytes.
  2. Call `rsm.submit(bytes).await`.
  3. If `Err(WrongLeader)` -> respond with `wrong_leader: true`.
  4. If Ok -> deserialize result and respond.

### File: `src/kv/client.rs` -- Phase 5B

```rust
/// Clerk keeps trying until it finds the leader.
pub struct Clerk {
    servers: Vec<KvClient>,   // Tonic clients to all KV servers
    leader_id: usize,         // last known leader
    client_id: String,        // unique, generated at creation
    seq_num: u64,             // monotonically increasing
}
```

**Retry loop for every operation:**
```rust
pub async fn get(&mut self, key: &str) -> String {
    self.seq_num += 1;
    loop {
        let resp = self.servers[self.leader_id].get(req).await;
        match resp {
            Ok(r) if !r.wrong_leader => return r.value,
            _ => {
                self.leader_id = (self.leader_id + 1) % self.servers.len();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
```

### Phase 5C: KV with Snapshots

**Add to `KvServer`/`Rsm`:**
- After applying each command, check `persister.raft_state_size()`.
- If it exceeds a configurable `max_raft_state` threshold:
  - Call `state_machine.snapshot()` to serialize the KV store + dup_table.
  - Call `raft.snapshot(last_applied_index, snapshot_data)`.

**`KvStateMachine::snapshot()`:** serialize `store` + `dup_table` with bincode.
**`KvStateMachine::restore(data)`:** deserialize and replace `store` + `dup_table`.

### File: `src/lib.rs`

Add `pub mod rsm;`

### File: `tests/kv_linearizability.rs`

**Test `test_kv_basic`:**
1. 3-node Raft cluster + KV servers. Create a Clerk.
2. `put("a", "1")`, `get("a")` -> assert "1".
3. `append("a", "2")`, `get("a")` -> assert "12".

**Test `test_kv_leader_failure`:**
1. 5-node cluster. Put some values.
2. Partition the leader.
3. Verify Clerk eventually finds new leader and operations succeed.
4. Heal partition. Verify consistency.

**Test `test_kv_concurrent`:**
1. 5-node cluster. 5 Clerks concurrently issuing Put/Append/Get.
2. Verify linearizability: the final state is consistent with SOME sequential
   ordering of all operations.

**Test `test_kv_unreliable`:**
1. 5-node cluster with message loss.
2. Multiple Clerks issuing operations.
3. Verify no duplicates (seq_num dedup), eventual consistency.

**Test `test_kv_snapshot`:**
1. 3-node cluster with `max_raft_state = 1000` bytes.
2. Issue enough Put operations to trigger multiple snapshots.
3. Crash and restart nodes. Verify they restore from snapshot.
4. Verify all operations still correct.

**How to run:** `cargo test --test kv_linearizability`

---

## Phase 6: Sharded KV

**Goal:** Partition the key space across multiple Raft groups for horizontal
scalability. A shard controller manages which group owns which shard.

*Detailed design to be written after Phase 5 is complete. High-level outline:*

### Architecture

```
                    ┌──────────────┐
                    │ ShardCtrler  │  (Raft group -- stores config)
                    │  (kvsrv)     │
                    └──────┬───────┘
                           │ Query/ChangeConfig
            ┌──────────────┼──────────────┐
            │              │              │
     ┌──────┴──────┐ ┌────┴────┐ ┌──────┴──────┐
     │ ShardGroup 1│ │ Group 2 │ │ ShardGroup 3│
     │ (Raft+KV)   │ │(Raft+KV)│ │ (Raft+KV)   │
     └─────────────┘ └─────────┘ └─────────────┘
```

- **10 shards** (fixed). `shard = hash(key) % 10`.
- **ShardConfig:** `{ num: u64, shards: [GroupId; 10], groups: HashMap<GroupId, Vec<ServerAddr>> }`
- **ShardCtrler:** Raft-replicated config store. Supports `Query(num)` and
  `ChangeConfigTo(new_config)`.
- **ShardGroup:** Raft+KV group that handles a subset of shards. Rejects
  requests for shards it doesn't own (`ErrWrongGroup`).

### Sub-phases

**6A -- Moving Shards:**
- Controller tells source group to `FreezeShard(shard, config_num)`.
- Source stops accepting writes for that shard.
- Controller copies shard data from source: `GetShard(shard, config_num)`.
- Controller installs at destination: `InstallShard(shard, data, config_num)`.
- Controller deletes at source: `DeleteShard(shard, config_num)`.
- Controller posts new config.
- All shard RPCs are fenced with `config_num` for idempotency.

**6B -- Handling Failed Controller:**
- Store "current" and "next" config. On startup, detect incomplete transitions
  and resume them.

**6C -- Concurrent Controllers:**
- Use versioned Puts to the config store so only one controller wins.
- `config_num` fencing in shard RPCs prevents stale controllers from causing harm.

### Files
- `src/shard_ctrler/mod.rs` -- ShardCtrler service
- `src/shard_kv/mod.rs` -- ShardGroup service (extends KV server with shard awareness)
- `tests/shard_*.rs` -- test files

---

## Incremental Testing Strategy

Run tests in this exact order. Do not move to the next phase until all tests
in the current phase pass reliably (run each test 10+ times).

```bash
# Phase 0 -- already passing
cargo test --test raft_ping

# Phase 1
cargo test --test raft_election

# Phase 2
cargo test --test raft_election      # must still pass!
cargo test --test raft_replication

# Phase 3
cargo test --test raft_election      # must still pass!
cargo test --test raft_replication   # must still pass!
cargo test --test raft_persistence

# Phase 4
cargo test --test raft_election      # must still pass!
cargo test --test raft_replication   # must still pass!
cargo test --test raft_persistence   # must still pass!
cargo test --test raft_snapshot

# Phase 5
cargo test --test kv_linearizability

# Phase 6
cargo test --test shard_basic
cargo test --test shard_concurrent

# Run everything
cargo test
```

**Stress testing** (run 50 times to catch concurrency bugs):
```bash
for i in $(seq 1 50); do
    echo "Run $i";
    cargo test --test raft_persistence || { echo "FAILED on run $i"; break; };
done
```

---

## Debugging Tips

1. **Structured logging:** Use `tracing` with node id and term in every message:
   ```rust
   tracing::info!(node = self.id, term = self.current_term, role = ?self.role, "event");
   ```

2. **Deterministic replay:** Turmoil with a fixed seed (`SmallRng::seed_from_u64`)
   gives reproducible runs. Change the seed to explore different orderings.

3. **Common bugs:**
   - Forgetting to reset election timer when granting a vote.
   - Not stepping down when seeing a higher term in ANY RPC response.
   - Committing entries from previous terms (violates Figure 8 safety).
   - Off-by-one in log indexing (use the helpers in `log.rs`).
   - Deadlock: blocking in the main `select!` loop on something that needs
     the main loop to make progress (use spawned tasks for RPCs).
   - Not persisting before responding to RPCs.

4. **Assertions:** Add `debug_assert!` liberally:
   ```rust
   debug_assert!(self.commit_index <= last_log_index(&self.log));
   debug_assert!(self.last_applied <= self.commit_index);
   ```
