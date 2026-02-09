use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use turmoil_raft::kv::client::Clerk;
use turmoil_raft::kv::server::KvOp;
use turmoil_raft::raft::core::RaftState;
use turmoil_raft::raft::log;

/// Shared sim-step counter. The test loop increments this at each `sim.step()`.
/// Hosts read it to timestamp operation boundaries.
pub type StepClock = Arc<AtomicU64>;

/// Shared append-only history of completed operations.
pub type History = Arc<Mutex<Vec<HistoryEntry>>>;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum HistoryOp {
    Get { key: String },
    Put { key: String, value: String },
    Append { key: String, value: String },
}

#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub client_id: String,
    pub seq_num: u64,
    pub op: HistoryOp,
    pub result: String,
    pub invoke_step: u64,
    pub complete_step: u64,
}

/// Wraps a Clerk and records every completed operation into a shared History.
pub struct TracedClerk {
    inner: Clerk,
    client_id: String,
    next_seq: u64,
    step_clock: StepClock,
    history: History,
}

impl TracedClerk {
    pub fn new(
        inner: Clerk,
        client_id: String,
        step_clock: StepClock,
        history: History,
    ) -> Self {
        Self {
            inner,
            client_id,
            next_seq: 0,
            step_clock,
            history,
        }
    }

    pub async fn get(&mut self, key: String) -> String {
        self.next_seq += 1;
        let seq_num = self.next_seq;
        let invoke_step = self.step_clock.load(Ordering::Relaxed);
        let result = self.inner.get(key.clone()).await;
        let complete_step = self.step_clock.load(Ordering::Relaxed);
        self.history.lock().unwrap().push(HistoryEntry {
            client_id: self.client_id.clone(),
            seq_num,
            op: HistoryOp::Get { key },
            result: result.clone(),
            invoke_step,
            complete_step,
        });
        result
    }

    pub async fn put(&mut self, key: String, value: String) {
        self.next_seq += 1;
        let seq_num = self.next_seq;
        let invoke_step = self.step_clock.load(Ordering::Relaxed);
        self.inner.put(key.clone(), value.clone()).await;
        let complete_step = self.step_clock.load(Ordering::Relaxed);
        self.history.lock().unwrap().push(HistoryEntry {
            client_id: self.client_id.clone(),
            seq_num,
            op: HistoryOp::Put { key, value },
            result: String::new(),
            invoke_step,
            complete_step,
        });
    }

    pub async fn append(&mut self, key: String, value: String) {
        self.next_seq += 1;
        let seq_num = self.next_seq;
        let invoke_step = self.step_clock.load(Ordering::Relaxed);
        self.inner.append(key.clone(), value.clone()).await;
        let complete_step = self.step_clock.load(Ordering::Relaxed);
        self.history.lock().unwrap().push(HistoryEntry {
            client_id: self.client_id.clone(),
            seq_num,
            op: HistoryOp::Append { key, value },
            result: String::new(),
            invoke_step,
            complete_step,
        });
    }
}

use turmoil_raft::raft::persist::Persister;

/// Verify that the recorded history is linearizable with respect to the
/// committed Raft log.
///
/// Algorithm:
/// 1. Find the server with the highest commit_index — its log is the most
///    complete view of the committed total order.
/// 2. Replay committed entries (with dedup) against a reference HashMap to
///    compute expected results.
/// 3. For every completed Get in the history, assert the client's observed
///    value matches the replay.
/// 4. For every pair (A, B) where A completed before B was invoked (real-time
///    ordering), assert A appears before B in the commit log.
pub fn check_linearizability(
    history: &[HistoryEntry],
    state_handles: &[Arc<Mutex<RaftState>>],
    canonical_log: &mut Vec<Option<KvOp>>,
) {
    // 1. Determine the global high-water mark for commits.
    let best_commit = state_handles
        .iter()
        .map(|h| h.lock().unwrap().commit_index)
        .max()
        .unwrap_or(0);

    tracing::info!(
        best_commit,
        canonical_len = canonical_log.len(),
        history_len = history.len(),
        "Starting linearizability check"
    );

    // 2. Sync canonical_log up to best_commit
    // We assume canonical_log[0] is the sentinel (index 0).
    while (canonical_log.len() as u64) <= best_commit {
        let next_idx = canonical_log.len() as u64;
        let mut found_cmd = None;

        // Try to find this index in any node's log
        for h in state_handles {
            let s = h.lock().unwrap();
            if let Some(entry) = log::entry_at(&s.log, next_idx) {
                found_cmd = Some(entry.command.clone());
                break;
            }
        }

        match found_cmd {
            Some(cmd) => {
                if cmd.is_empty() {
                    canonical_log.push(None); // No-op or empty command
                } else {
                    match bincode::deserialize(&cmd) {
                        Ok(op) => canonical_log.push(Some(op)),
                        Err(_) => canonical_log.push(None), // Should not happen for valid KvOp
                    }
                }
            }
            None => {
                // If we are here, it means next_idx <= best_commit, but NO node has it in log.
                // This implies all nodes have compacted it.
                // Since we are incrementally building, this implies the test harness missed it.
                // This is a fatal error for the test infrastructure.
                panic!(
                    "Test harness gap: committed index {} is missing from all nodes (compacted) \
                     and not in canonical log. Current canonical len: {}",
                    next_idx,
                    canonical_log.len()
                );
            }
        }
    }

    // 3. Replay canonical log to build state
    let mut ref_store: HashMap<String, String> = HashMap::new();
    let mut dedup: HashMap<String, u64> = HashMap::new();
    let mut commit_map: HashMap<(String, u64), (usize, String)> = HashMap::new();

    for (i, op_opt) in canonical_log.iter().enumerate() {
        if i == 0 { continue; } // Skip sentinel
        let op = match op_opt {
            Some(o) => o,
            None => continue,
        };

        let (client_id, seq_num) = match op {
            KvOp::Get { client_id, seq_num, .. } => (client_id, seq_num),
            KvOp::Put { client_id, seq_num, .. } => (client_id, seq_num),
            KvOp::Append { client_id, seq_num, .. } => (client_id, seq_num),
        };

        // Dedup
        let last_seq = *dedup.get(client_id).unwrap_or(&0);
        if *seq_num <= last_seq {
            continue;
        }
        dedup.insert(client_id.clone(), *seq_num);

        let result = match op {
            KvOp::Get { key, .. } => ref_store.get(key).cloned().unwrap_or_default(),
            KvOp::Put { key, value, .. } => {
                ref_store.insert(key.clone(), value.clone());
                String::new()
            }
            KvOp::Append { key, value, .. } => {
                ref_store.entry(key.clone()).or_default().push_str(value);
                String::new()
            }
        };

        commit_map.insert((client_id.clone(), *seq_num), (i, result));
    }

    // 4. Verify result correctness
    let mut matched = 0;
    let mut unmatched = 0;

    for h in history {
        let key = (h.client_id.clone(), h.seq_num);
        if let Some((_, expected_result)) = commit_map.get(&key) {
            matched += 1;
            if let HistoryOp::Get { .. } = &h.op {
                assert_eq!(
                    h.result, *expected_result,
                    "Linearizability violation (result mismatch): client '{}' seq {} \
                    Get observed '{}' but commit-order replay says '{}'",
                    h.client_id, h.seq_num, h.result, expected_result
                );
            }
        } else {
            unmatched += 1;
        }
    }

    // 5. Check real-time ordering
    for (ai, a) in history.iter().enumerate() {
        let a_order = match commit_map.get(&(a.client_id.clone(), a.seq_num)) {
            Some((o, _)) => *o,
            None => continue,
        };

        for (bi, b) in history.iter().enumerate() {
            if ai == bi { continue; }
            if a.complete_step >= b.invoke_step { continue; }

            let b_order = match commit_map.get(&(b.client_id.clone(), b.seq_num)) {
                Some((o, _)) => *o,
                None => continue,
            };

            if a_order == b_order { continue; }

            assert!(
                a_order < b_order,
                "Linearizability violation (real-time ordering): \
                op ({}, seq {}) completed at step {} before op ({}, seq {}) \
                invoked at step {}, but commit order {} >= {}",
                a.client_id, a.seq_num, a.complete_step,
                b.client_id, b.seq_num, b.invoke_step,
                a_order, b_order
            );
        }
    }

    tracing::info!(matched, unmatched, "Linearizability check PASSED");
}
