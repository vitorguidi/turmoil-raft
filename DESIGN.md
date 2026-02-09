# Turmoil Raft: Design & Architecture

Raft consensus + linearizable KV store in Rust, tested via
[Turmoil](https://github.com/tokio-rs/turmoil) deterministic simulation.
Inspired by MIT 6.5840 (formerly 6.824).

## Status

| Phase | Description | Status |
|-------|-------------|--------|
| 0 | Networking & boilerplate | Done |
| 1 | Leader election | Done |
| 2 | Log replication | Done |
| 3 | Persistence & fast backup | Done |
| 4 | Log compaction / snapshots | Done |
| 5 | Fault-tolerant KV service | Done |

---

## Architecture

```
                      ┌──────────────────────────────────────────────┐
                      │                   Server Host                │
                      │                                              │
  Clerk ──gRPC──────▶ │  KvServer ──RaftMsg::Start──▶ Raft core     │
  (retry loop)        │     │                            │           │
                      │     │◀── ApplyMsg::Command ──────┘           │
                      │     │◀── ApplyMsg::Snapshot ─────┘           │
                      │     │                                        │
                      │     │──RaftMsg::Snapshot──▶ Raft core        │
                      │     │   (log compaction trigger)             │
                      │     ▼                                        │
                      │  HashMap<K,V>  (state machine)               │
                      │  dup_table     (deduplication)               │
                      │                                              │
                      │  Raft core ──gRPC──▶ Peer Raft cores        │
                      │     │         (AppendEntries / InstallSnapshot)
                      │     ▼                                        │
                      │  Persister (Arc<Mutex<>>)                    │
                      │  (raft state + snapshot data)                │
                      │  (survives turmoil crash/bounce)             │
                      └──────────────────────────────────────────────┘
```

Every server host runs both a Raft service and a KV service on the same
tonic gRPC server (port 9000). Clerks connect to all servers and retry
across them until they find the current leader.

---

## Source Layout

```
src/
├── lib.rs                  Module root
├── pb/mod.rs               Generated protobuf code (raft + kv)
├── raft/
│   ├── mod.rs
│   ├── core.rs             Raft state machine & event loop
│   ├── log.rs              Log indexing helpers (sentinel, term_at, etc.)
│   ├── rpc.rs              tonic gRPC handlers → dispatch to core via channels
│   ├── client.rs           RaftClient with reconnect-on-error
│   └── persist.rs          In-memory Persister (bincode serialization)
└── kv/
    ├── mod.rs
    ├── server.rs           KvServer: gRPC handlers, applier loop, state machine
    └── client.rs           Clerk: retry loop with leader tracking

proto/
├── raft.proto              RequestVote, AppendEntries, InstallSnapshot RPCs
└── kv.proto                Get, PutAppend RPCs

tests/
├── common/
│   ├── mod.rs              Turmoil ↔ tonic bridge, sim loop, GCS upload
│   ├── config.rs           SimConfig (Default + random() for fuzzing)
│   ├── oracle.rs           Raft safety invariants (4 properties)
│   ├── linearizability_oracle.rs   TracedClerk + linearizability checker
│   ├── raft_sim.rs         Raft-only simulation setup
│   └── kv_sim.rs           Raft + KV simulation setup
├── raft_simulation.rs      Raft-only deterministic simulation test
├── kv_linearizability.rs   KV linearizability test with crashes
└── simulation_harness.rs   Continuous fuzzing harness (local + k8s)

k8s/
└── k8s-deployment.yaml.tmpl   10-replica fuzzer deployment + GCP secret

Dockerfile                  Multi-stage build for fuzzer binary
```

---

## Raft Core (`src/raft/core.rs`)

### State

```
RaftState (shared via Arc<Mutex<>>):
  term, voted_for, role          Persistent (Figure 2)
  log: Vec<LogEntry>             Persistent; sentinel at index 0
  commit_index, last_applied     Volatile

Raft (private, owned by event loop):
  peers: BTreeMap<u64, RaftClient>
  next_index, match_index        Volatile leader state
  persister: Arc<Mutex<Persister>>
  apply_tx                       Channel to KV applier (Command + Snapshot)
  received_votes                 Vote counter during elections
```

`RaftState` is behind `Arc<Mutex<>>` so the test oracle can inspect it
from outside the host without going through RPCs.

### Event Loop

The core runs a single `tokio::select! { biased; ... }` loop:

1. **Message received** (highest priority) — RPC request/response or client
   `Start` command. Dispatched to `handle_*` methods.
2. **Election timeout** — becomes candidate, increments term, requests votes.
3. **Heartbeat tick** — leader sends AppendEntries to all peers.

After every iteration: `apply_committed()` sends newly committed entries
to the KV layer via `apply_tx`.

### Key Invariants

- **Figure 8 safety**: `maybe_advance_commit_index()` only commits entries
  whose term equals the leader's current term.
- **Persistence**: `persist()` is called after every mutation to term,
  voted_for, or log — before responding to any RPC.
- **Fast backup**: On AppendEntries rejection, the follower returns
  `conflict_term` and `conflict_index`. The leader skips back by entire
  terms instead of decrementing next_index one at a time.

### RPC Dispatch

All three RPCs (`RequestVote`, `AppendEntries`, `InstallSnapshot`) follow
the same pattern in `rpc.rs`:

1. tonic handler receives protobuf request
2. Creates a oneshot channel
3. Sends `RaftMsg::*Req { req, reply }` to the core's mpsc channel
4. Awaits the oneshot response
5. Returns protobuf response to caller

This keeps all state mutations on a single task (no locking needed inside
the core loop).

---

## Tonic + Turmoil Bridge (`tests/common/mod.rs`)

Turmoil simulates the network but doesn't provide a real TCP stack.
The bridge code makes tonic work inside turmoil:

- **`incoming::Accepted`** wraps `turmoil::net::TcpStream` to implement
  `AsyncRead`/`AsyncWrite`/`Connected` for tonic's server.
- **`connector::connector()`** is a `tower::Service` that creates turmoil
  TCP connections from a URI, used by tonic's client.
- **`listener_stream()`** converts a turmoil `TcpListener` into a
  `ReceiverStream` that tonic's server can accept connections from.
- **`create_channel()`** creates a lazy tonic `Channel` with HTTP/2
  keepalive (500ms interval, 1s timeout) to detect dead connections
  after partitions heal.

### HTTP/2 Connection Corruption Caveat

Turmoil's `crash()` destroys the server-side TCP state, but the client's
HTTP/2 connection object doesn't know this. After a `bounce()`, the old
connection is corrupted — tonic sees `h2` protocol errors or hangs.

**Workaround** (`src/raft/client.rs`): `RaftClient` wraps the tonic client
with a `ChannelFactory`. On any transport error (`Unavailable`, `Internal`),
it replaces the entire channel with a fresh TCP connection:

```rust
pub fn reconnect(&mut self) {
    let new_channel = (self.channel_factory)();
    self.inner = TonicRaftClient::new(new_channel);
}
```

The same pattern is used in `Clerk` (`src/kv/client.rs`). This is not a
retry — Raft's heartbeat cycle naturally retries the RPC on the next tick.

**Keepalive** is configured on channels to detect stale connections:
```rust
.http2_keep_alive_interval(Duration::from_millis(500))
.keep_alive_timeout(Duration::from_millis(1000))
```

Without keepalive, a healed partition could leave dead connections open
indefinitely, preventing new RPCs from reaching the peer.

---

## KV Service

### KvServer (`src/kv/server.rs`)

The KV server owns the state machine and talks to Raft through channels:

**Request path** (`submit_and_wait`):
1. Serialize `KvOp` (Get/Put/Append + client_id + seq_num) to bincode
2. Send `RaftMsg::Start { command, reply }` to Raft core
3. If not leader → return `wrong_leader`
4. Register a `PendingOp { term, tx }` in the pending map, keyed by log index
5. Await the oneshot with 5-second timeout

**Apply path** (`run_applier`, spawned as background task):
1. Receive `ApplyMsg::Command { index, term, command }` from Raft
2. Deserialize `KvOp`, check dup_table (client_id → last seq_num)
3. If duplicate → return cached result
4. Execute on `HashMap<String, String>` store
5. Update dup_table
6. If pending map has an entry at this index with matching term → send result
7. If term mismatch → drop the oneshot (caller gets `wrong_leader`)

**Deduplication**: Every `KvOp` carries `client_id` + `seq_num`. The
dup_table maps `client_id → (last_seq_num, cached_result)`. If a clerk
retries (same seq_num), the server returns the cached result instead of
re-executing. This prevents double-applies on leader failover.

**Every Get goes through Raft** — no read-only optimization. This ensures
linearizability: a stale leader cannot serve reads from its local state.

### Clerk (`src/kv/client.rs`)

```
Clerk:
  servers: Vec<(ChannelFactory, KvClient)>
  leader_id: usize       Last known leader (round-robin on failure)
  client_id: String       UUID, stable across retries
  seq_num: u64            Monotonically increasing per operation
```

Every operation (get, put, append):
1. Increment seq_num
2. Send RPC to current leader_id
3. If `wrong_leader` or RPC error → `next_leader()` (round-robin), sleep
   100ms, retry
4. On RPC error → reconnect the channel (turmoil connection corruption)

---

## Log Compaction & Snapshots

### Offset-based Log (`src/raft/log.rs`)

The log is a `Vec<LogEntry>` with a sentinel at position 0. Initially
`log[0].index == 0`. After a snapshot compacts entries up to index N,
the log is trimmed and `log[0]` becomes a new sentinel with
`index = N, term = term_at_N`. All entries before the sentinel are
discarded — they're covered by the snapshot.

Because vec indices no longer equal Raft log indices, three helpers
translate between the two:

```
offset(log)              → log[0].index (first global index)
vec_index(log, global)   → Some(global - offset) if in range, else None
entry_at(log, global)    → &log[vec_index] if in range
```

Every log access in `core.rs` goes through these helpers. Direct
`core.log[index as usize]` would panic after compaction.

### Snapshot Trigger (KV → Raft)

After applying each committed command, the KV applier checks:

```
if max_raft_state > 0 && persister.raft_state_size() >= max_raft_state {
    serialize (store, dup_table) with bincode
    send RaftMsg::Snapshot { index, data } to Raft
}
```

`max_raft_state` controls how large the persisted raft state (log) can
grow before a snapshot is triggered. Smaller values = more frequent
snapshots. The fuzzer uses 500–2000 bytes to stress snapshot paths.
Set to 0 to disable snapshots (raft-only sim).

### Log Compaction (Raft handles RaftMsg::Snapshot)

When Raft receives `RaftMsg::Snapshot { index, data }`:

1. Ignore if `index <= offset` (already compacted past this point)
2. Look up the term at `index` in the current log
3. Split the log: keep entries after `index`, replace `log[0]` with a
   new sentinel `{ index, term }`
4. Persist both the trimmed raft state and the snapshot data

### InstallSnapshot RPC (Leader → Follower)

In `send_append_entries()`, when the leader detects that a peer's
`next_index <= log::offset()`, the peer is too far behind for
AppendEntries — the entries it needs have been compacted away. Instead,
the leader sends the full snapshot via `InstallSnapshot` RPC:

```
InstallSnapshotRequest {
    term, leader_id,
    last_included_index: offset,
    last_included_term: log[0].term,
    data: persister.read_snapshot()
}
```

The follower (`handle_install_snapshot_req`):
1. Checks term (become_follower if higher term)
2. Skips if `last_included_index <= own offset` (already have it)
3. Trims own log: keeps entries after the snapshot index if any,
   otherwise resets to just the new sentinel
4. Advances `last_applied` and `commit_index` to the snapshot index
5. Persists raft state + snapshot
6. Sends `ApplyMsg::Snapshot { data }` to KV applier, which replaces
   its store and dup_table with the deserialized snapshot

### Snapshot Restore on Boot

In `Raft::new`, after restoring persistent raft state (term, voted_for,
log), the constructor also reads the snapshot from the persister. If a
snapshot exists, it sets `commit_index = last_applied = offset` and
sends `ApplyMsg::Snapshot` to the KV applier. This works because
`KvServer::new` (which spawns the applier) is called before `Raft::new`
in the sim setup.

---

## Persistence (`src/raft/persist.rs`)

Turmoil does not simulate a filesystem. Instead, persistence uses an
in-memory `Persister` shared via `Arc<Mutex<>>` between the Raft core
and the test harness. Turmoil preserves host-local `Arc` state across
`crash()`/`bounce()`, so the persisted data survives.

**What's persisted** (per Figure 2):
- `current_term`
- `voted_for`
- `log: Vec<LogEntry>` (post-compaction: only entries after the snapshot)
- `snapshot: Vec<u8>` (bincode-serialized KV state machine)

**Serialization**: bincode. LogEntry is prost-generated (no serde), so
entries are converted to `(index, term, command)` tuples for encoding.
Snapshots are stored as raw bytes via `save_snapshot()`/`read_snapshot()`.

**When persisted**: After every mutation to term, voted_for, or log, and
before responding to any RPC. Snapshot data is persisted alongside raft
state when log compaction occurs (either from KV trigger or
InstallSnapshot RPC).

`raft_state_size()` returns the byte length of the serialized raft state
(excluding snapshot). The KV layer uses this to decide when to trigger
a snapshot.

---

## Testing Strategy

### 1. Safety Oracles (`tests/common/oracle.rs`)

The `Oracle` holds `Arc<Mutex<RaftState>>` handles for every node and
checks four Raft safety properties after every `sim.step()`:

| Property | Check |
|----------|-------|
| **Election safety** | At most one leader per term (point-in-time snapshot) |
| **Log matching** | Entries with same (index, term) have identical commands (offset-aware: only compares indices present in both logs) |
| **State machine safety** | If two servers have applied entry at index N, they applied the same entry (skips compacted indices) |
| **Leader completeness** | Current leader's log contains all previously committed entries (skips indices below leader's snapshot offset) |

Leader completeness ignores "zombie leaders" — nodes with `role == Leader`
but whose term is less than the highest term in the cluster. These are
stale leaders that haven't yet learned about the new term.

### 2. Linearizability Oracle (`tests/common/linearizability_oracle.rs`)

`TracedClerk` wraps `Clerk` and records every operation to an append-only
`History` with `invoke_step` and `complete_step` timestamps (from a
sim-step counter, not wall clock).

`check_linearizability()` runs after every new history entry:

1. Find the server with the highest `commit_index`
2. Replay committed log entries with dedup (matching server behavior)
3. For each Get in the history, verify the returned value matches the
   replayed state
4. **Real-time ordering**: if operation A completes before operation B
   starts (`A.complete_step < B.invoke_step`), then A must appear before
   B in the commit log

### 3. Deterministic Simulation

All tests use Turmoil for deterministic execution:

- Virtual clock (no real-time waits)
- Seeded RNG (`SmallRng::seed_from_u64`) for reproducible fault injection
- `sim.step()` advances the simulation one tick at a time
- Configurable network latency and failure rates

**SimConfig** (`tests/common/config.rs`) captures all parameters needed
to reproduce a test run:

```rust
SimConfig {
    seed, max_steps,
    latency_avg, network_fail_rate,
    crash_probability, bounce_delay,
    election_jitter, election_interval, heartbeat_interval,
    nr_nodes, nr_clients,
    max_raft_state,  // 0 = no snapshots, >0 = snapshot when raft state exceeds this many bytes
}
```

Same seed + same config = identical execution trace (verified empirically).

### 4. Fault Injection

The sim loop (`run_sim_loop` / `run_sim_loop_kv`) injects faults:

- **Crashes**: At each step, with probability `crash_probability`, crash a
  random node via `sim.crash()`. Reset its volatile state (commit_index
  and last_applied to the log's snapshot offset, role to Follower).
- **Bounces**: After `bounce_delay` steps, restart the crashed node via
  `sim.bounce()`. It restores persistent state from the `Persister`.
- **Quorum preservation**: At most 1 node down at a time (maintains
  majority for progress). The KV linearizability test allows up to N-1
  concurrent failures.
- **Network failures**: Turmoil's `sim.set_fail_rate()` drops messages
  randomly.

### 5. Test Binaries

| Test | What it tests |
|------|---------------|
| `cargo test --test raft_simulation` | Raft-only: elections, replication, persistence with crash/bounce. No KV layer. |
| `cargo test --test kv_linearizability` | Full stack: Raft + KV + clients. Linearizability + safety oracles with crash/bounce. |
| `cargo test --test simulation_harness` | Fuzzing harness (runs forever). Random configs, crash reports saved to disk + GCS. |

### 6. Continuous Fuzzing (k8s)

The `simulation_harness` test is packaged as a Docker image and deployed
to Kubernetes (kind cluster) as 10 replicas:

```
                  ┌──────────────────────────┐
                  │    k8s Deployment (10x)   │
                  │                           │
                  │  ┌────────────────────┐   │
                  │  │  fuzzer binary      │   │
                  │  │  (infinite loop)    │   │
                  │  │                     │   │
                  │  │  crash? ──▶ JSON    │   │
                  │  │           ──▶ GCS   │   │
                  │  └────────────────────┘   │
                  └──────────────────────────┘
```

**Fuzzing loop** (`simulation_harness.rs`):
1. Generate random `SimConfig`
2. Run simulation inside `catch_unwind`
3. On panic (invariant violation): save config as JSON, upload to GCS
4. Repeat forever

**Crash reproduction**:
```bash
FUZZ_MODE=reproduce FUZZ_FILE=crash-12345.json cargo test --test simulation_harness
```

The JSON contains the full `SimConfig`, so the exact fault sequence is
replayed deterministically.

**GCS upload** uses the `cloud-storage` crate, which reads credentials
from the `SERVICE_ACCOUNT` env var (not `GOOGLE_APPLICATION_CREDENTIALS`).
The upload is wrapped in `catch_unwind` to prevent credential/network
issues from killing the fuzz loop.

---

## Known Caveats & Limitations

### Full snapshots only
Snapshots are always sent as a single message (no chunking). This is
fine for simulation (small state), but a production deployment with
large state machines would need chunked transfer.

### Dup table grows unbounded
`KvServer.dup_table` stores the last seq_num and result for every client
that has ever issued a request. It's never pruned. In a long-running
fuzzer session this is fine (few clients), but production use would need
pruning (e.g., on snapshot).

### Apply channel backpressure
`apply_committed()` uses `try_send` on the apply channel. If the KV
applier falls behind, committed entries can be dropped. In practice this
hasn't been observed because the applier processes entries faster than
Raft commits them.

### Turmoil TCP after crash
After `sim.crash()` + `sim.bounce()`, existing TCP connections are dead
but clients don't know. Both `RaftClient` and `Clerk` work around this
by replacing the entire tonic channel on any transport error. The
connection is not retried — Raft's heartbeat cycle or the clerk's retry
loop handles the next attempt.

### No real filesystem
`Persister` is in-memory. It models crash-recovery correctly for
simulation (state survives `crash()`/`bounce()` via `Arc`), but doesn't
test real fsync semantics.

### Single-node crash limit in Raft sim
`run_sim_loop()` limits crashes to 1 concurrent node to always maintain
quorum. The KV linearizability test (`kv_linearizability.rs`) allows up
to N-1 concurrent failures, which can cause temporary liveness loss but
should not violate safety.

### Biased select
The main Raft loop uses `tokio::select! { biased; ... }` to prioritize
message processing over timeout handling. This prevents the election
timer from firing while there are queued messages, reducing spurious
elections.

---

## Running

```bash
# Raft-only simulation (default config, ~30s)
cargo test --test raft_simulation -- --nocapture

# KV linearizability (5 nodes, 3 clients, crashes, ~60s)
cargo test --test kv_linearizability -- --nocapture

# Local fuzz session (runs forever, Ctrl-C to stop)
RUST_LOG=warn cargo test --test simulation_harness -- --nocapture

# Reproduce a crash
FUZZ_MODE=reproduce FUZZ_FILE=crash-12345.json RUST_LOG=info \
  cargo test --test simulation_harness -- --nocapture

# k8s fuzzing (requires kind cluster + GCP credentials)
# See k8s/k8s-deployment.yaml.tmpl for template variables
```
