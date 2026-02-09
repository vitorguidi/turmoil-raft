use serde::{Serialize, Deserialize};
use crate::pb::raft::LogEntry;
use std::collections::BTreeMap;

/// Serializable persistent state (Figure 2: currentTerm, votedFor, log[]).
///
/// LogEntry is a prost-generated type without serde support, so we convert
/// to tuples for serialization.
#[derive(Serialize, Deserialize)]
struct PersistentState {
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<(u64, u64, Vec<u8>)>, // (index, term, command)
    debug_log: BTreeMap<u64, Vec<u8>>,
}

/// In-memory persistence that survives simulated crashes.
///
/// Turmoil doesn't mock the filesystem, so we use this `Arc<Mutex<Persister>>`
/// shared between the test harness and the Raft core. The persister is created
/// outside the host closure, so it survives crash/bounce cycles.
#[derive(Debug)]
pub struct Persister {
    raft_state: Vec<u8>,
    snapshot: Vec<u8>,
}

impl Persister {
    pub fn new() -> Self {
        Self {
            raft_state: Vec::new(),
            snapshot: Vec::new(),
        }
    }

    pub fn save_raft_state(&mut self, data: Vec<u8>) {
        self.raft_state = data;
    }

    pub fn read_raft_state(&self) -> &[u8] {
        &self.raft_state
    }

    pub fn save_snapshot(&mut self, data: Vec<u8>) {
        self.snapshot = data;
    }

    pub fn read_snapshot(&self) -> &[u8] {
        &self.snapshot
    }

    pub fn raft_state_size(&self) -> usize {
        self.raft_state.len()
    }
}

/// Encode persistent Raft state (term, voted_for, log) to bytes.
pub fn encode(current_term: u64, voted_for: Option<u64>, log: &[LogEntry], debug_log: &BTreeMap<u64, Vec<u8>>) -> Vec<u8> {
    let state = PersistentState {
        current_term,
        voted_for,
        log: log.iter().map(|e| (e.index, e.term, e.command.clone())).collect(),
        debug_log: debug_log.clone(),
    };
    bincode::serialize(&state).expect("bincode serialize failed")
}

/// Decode persistent Raft state from bytes. Returns (term, voted_for, log).
pub fn decode(data: &[u8]) -> (u64, Option<u64>, Vec<LogEntry>, BTreeMap<u64, Vec<u8>>) {
    let state: PersistentState = bincode::deserialize(data).expect("bincode deserialize failed");
    let log = state.log.into_iter().map(|(index, term, command)| {
        LogEntry { index, term, command }
    }).collect();
    (state.current_term, state.voted_for, log, state.debug_log)
}
