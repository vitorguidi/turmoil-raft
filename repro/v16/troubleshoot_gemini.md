# Implementation Lessons (Troubleshooting Log)

### 1. Snapshot Persistence Ordering
**Crash Safety:** We must save the snapshot **before** truncating the Raft log.
- *Bad:* Truncate Log → Crash → Save Snapshot. Result: Disk has truncated log (starts at index 100) but old snapshot (ends at 50). Indices 51-99 are lost (gap).
- *Good:* Save Snapshot → Truncate Log. Result: Overlap. The state machine handles deduplication safely.

### 2. Async Snapshot Application
**Race Condition:** `InstallSnapshot` updates `last_applied` immediately, but the snapshot is sent to the KV layer over an async channel.
- *Symptom:* A `Get` request arrives, sees updated `last_applied`, and reads from the KV store *before* the snapshot is applied. Returns stale data (Linearizability violation).
- *Fix:* `handle_install_snapshot` must `await` the channel send to the KV layer before updating `last_applied`.

### 3. ReadIndex & The No-Op Entry
**Raft Safety:** A new leader cannot know the commit status of entries from previous terms until it commits an entry in its *current* term (Paper §6.4).
- *Symptom:* Stale reads immediately after election.
- *Fix:* Leader appends and commits a **no-op entry** upon election. `ReadIndex` is rejected until this entry is committed.

### 4. Oracle vs. Compaction
**Test Harness:** The linearizability oracle needs the full history of committed commands to verify execution, but compaction deletes them.
- *Fix:* Added a `debug_log` (persisted BTreeMap) to `RaftState`. It retains commands even after compaction.
- *Sentinel:* The oracle must handle the sentinel entry (index 0, empty command) by looking up the actual command in `debug_log`.
- *Pruning:* When installing a snapshot, `debug_log` must be pruned to remove divergent entries from minority forks, preventing the oracle from seeing "phantom" commits.

### 5. Zombie Leaders
**Oracle Logic:** The oracle used to trust any node claiming `role == Leader`.
- *Issue:* A partitioned leader (term 5) stays leader while the cluster moves to term 10. The zombie's log diverges.
- *Fix:* Oracle ignores leaders whose term is less than the cluster's max observed term.