use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use turmoil_raft::kv::client::Clerk;
use turmoil_raft::kv::server::KvOp;
use turmoil_raft::raft::core::RaftState;

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
    persisters: &[Arc<Mutex<Persister>>],
) {
    // 1. Find the server with the highest commit_index.
    let (best_idx, best_commit) = state_handles
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let s = h.lock().unwrap();
            (i, s.commit_index)
        })
        .max_by_key(|&(_, ci)| ci)
        .unwrap();

    let best_state = state_handles[best_idx].lock().unwrap();
    let log = &best_state.log;
    let log_offset = log[0].index; // log[0] is sentinel, its index is the offset

    tracing::info!(
        server = best_idx + 1,
        commit_index = best_commit,
        log_len = log.len(),
        log_offset,
        history_len = history.len(),
        "Starting linearizability check"
    );

    // 2. Replay committed entries with dedup.
    // Initialize from snapshot if available
    let mut ref_store: HashMap<String, String> = HashMap::new();
    let mut dedup: HashMap<String, u64> = HashMap::new(); // client_id -> last applied seq

    {
        let p = persisters[best_idx].lock().unwrap();
        let snap_data = p.read_snapshot();
        if !snap_data.is_empty() {
            match bincode::deserialize::<(
                HashMap<String, String>,
                HashMap<String, (u64, String)>,
            )>(snap_data)
            {
                Ok((snap_store, snap_dup)) => {
                    ref_store = snap_store;
                    // Transform snap_dup (value has result string) to our dedup (just seq)
                    // Actually, we might need the result if we were validating that too,
                    // but here we just need seq to skip duplicates.
                    dedup = snap_dup.into_iter().map(|(k, v)| (k, v.0)).collect();
                    tracing::info!("Restored state from snapshot for linearizability check");
                }
                Err(e) => tracing::error!("Failed to deserialize snapshot for linearizability check: {}", e),
            }
        }
    }

    // (client_id, seq_num) -> (commit_order_index, expected_result)
    let mut commit_map: HashMap<(String, u64), (usize, String)> = HashMap::new();

    // Replay log from offset + 1 to commit_index
    for i in (log_offset + 1)..=best_commit {
        // Map Raft index 'i' to vec index
        let vec_idx = (i - log_offset) as usize;
        if vec_idx >= log.len() {
            break;
        }
        let entry = &log[vec_idx];
        
        // Sanity check
        if entry.index != i {
            panic!("Log index mismatch: expected {}, got {}", i, entry.index);
        }

        let op: KvOp = match bincode::deserialize(&entry.command) {
            Ok(op) => op,
            Err(_) => continue, // skip non-KV entries
        };

        let (client_id, seq_num) = match &op {
            KvOp::Get {
                client_id,
                seq_num,
                ..
            }
            | KvOp::Put {
                client_id,
                seq_num,
                ..
            }
            | KvOp::Append {
                client_id,
                seq_num,
                ..
            } => (client_id.clone(), *seq_num),
        };

        // Dedup: skip if this client+seq was already applied
        if let Some(&last_seq) = dedup.get(&client_id) {
            if seq_num <= last_seq {
                continue;
            }
        }

        let result = match &op {
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

        dedup.insert(client_id.clone(), seq_num);
        commit_map.insert((client_id, seq_num), (i as usize, result));
    }

    // 3. Verify result correctness for every history entry.
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
            tracing::warn!(
                client_id = %h.client_id,
                seq_num = h.seq_num,
                "History entry not found in committed log"
            );
        }
    }

    tracing::info!(matched, unmatched, "Result correctness check done");

    // 4. Check real-time ordering.
    for (ai, a) in history.iter().enumerate() {
        let a_order = match commit_map.get(&(a.client_id.clone(), a.seq_num)) {
            Some((o, _)) => *o,
            None => continue,
        };

        for (bi, b) in history.iter().enumerate() {
            if ai == bi {
                continue;
            }
            // A completed before B was invoked => A must be ordered before B
            if a.complete_step >= b.invoke_step {
                continue;
            }

            let b_order = match commit_map.get(&(b.client_id.clone(), b.seq_num)) {
                Some((o, _)) => *o,
                None => continue,
            };

            assert!(
                a_order < b_order,
                "Linearizability violation (real-time ordering): \
                 op ({}, seq {}) completed at step {} before op ({}, seq {}) \
                 invoked at step {}, but commit order {} >= {}",
                a.client_id,
                a.seq_num,
                a.complete_step,
                b.client_id,
                b.seq_num,
                b.invoke_step,
                a_order,
                b_order
            );
        }
    }

    tracing::info!(
        history_entries = history.len(),
        committed = best_commit,
        matched,
        "Linearizability check PASSED"
    );
}
