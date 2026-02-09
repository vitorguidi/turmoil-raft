use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use turmoil_raft::raft::core::{RaftState, Role};
use turmoil_raft::raft::log;

pub struct Oracle {
    handles: Vec<Arc<Mutex<RaftState>>>,
}

impl Oracle {
    pub fn new(handles: Vec<Arc<Mutex<RaftState>>>) -> Self {
        Self { handles }
    }

    pub fn assert_invariants(&self) {
        self.assert_election_safety();
        self.assert_log_matching();
        self.assert_state_machine_safety();
        self.assert_leader_completeness();
    }

    /// At most one leader per term.
    pub fn assert_election_safety(&self) {
        let mut leaders_per_term: BTreeMap<u64, u64> = BTreeMap::new();
        for state in &self.handles {
            let s = state.lock().unwrap();
            if s.role == Role::Leader {
                *leaders_per_term.entry(s.term).or_insert(0) += 1;
            }
        }

        for (term, count) in leaders_per_term {
            assert!(count <= 1, "Election safety violation: {} leaders found in term {}", count, term);
        }
    }

    /// Log Matching Property: if two entries have the same index AND same term,
    /// they must store the same command. Different terms at the same index are
    /// normal during log divergence (e.g., after a partition/leader change).
    ///
    /// After log compaction, logs may have different offsets. Only compare
    /// entries that both servers have (intersection of their index ranges).
    pub fn assert_log_matching(&self) {
        let states: Vec<_> = self.handles.iter()
            .map(|h| h.lock().unwrap())
            .collect();

        for i in 0..states.len() {
            for j in (i + 1)..states.len() {
                let off_i = log::offset(&states[i].log);
                let off_j = log::offset(&states[j].log);
                let start = std::cmp::max(off_i, off_j);

                let last_i = log::last_log_index(&states[i].log);
                let last_j = log::last_log_index(&states[j].log);
                let end = std::cmp::min(last_i, last_j);

                for idx in start..=end {
                    let ei = match log::entry_at(&states[i].log, idx) {
                        Some(e) => e,
                        None => continue,
                    };
                    let ej = match log::entry_at(&states[j].log, idx) {
                        Some(e) => e,
                        None => continue,
                    };
                    if ei.index == ej.index && ei.term == ej.term && ei.command != ej.command {
                        panic!(
                            "Log matching violation at index {} term {}: server {} and server {} have different commands",
                            ei.index, ei.term, i + 1, j + 1
                        );
                    }
                }
            }
        }
    }

    /// For any index that two servers have both committed, the entries must be identical.
    /// Entries below a server's log offset are in its snapshot and cannot be individually checked.
    pub fn assert_state_machine_safety(&self) {
        let states: Vec<_> = self.handles.iter()
            .map(|h| h.lock().unwrap())
            .collect();

        for i in 0..states.len() {
            for j in (i + 1)..states.len() {
                let committed = std::cmp::min(states[i].commit_index, states[j].commit_index);
                // Start from the max offset — entries below are in snapshots
                let start = std::cmp::max(
                    log::offset(&states[i].log),
                    log::offset(&states[j].log),
                ) + 1; // +1 to skip sentinel

                for idx in start..=committed {
                    let ei = match log::entry_at(&states[i].log, idx) {
                        Some(e) => e,
                        None => break,
                    };
                    let ej = match log::entry_at(&states[j].log, idx) {
                        Some(e) => e,
                        None => break,
                    };
                    assert_eq!(
                        ei.term, ej.term,
                        "State machine safety violation at committed index {}: server {} term {}, server {} term {}",
                        idx, i + 1, ei.term, j + 1, ej.term
                    );
                    assert_eq!(
                        ei.command, ej.command,
                        "State machine safety violation at committed index {}: different commands on server {} and server {}",
                        idx, i + 1, j + 1
                    );
                }
            }
        }
    }

    /// The current leader's log must contain all entries that any server considers committed.
    /// Entries below the leader's log offset are covered by its snapshot.
    pub fn assert_leader_completeness(&self) {
        let states: Vec<_> = self.handles.iter()
            .map(|h| h.lock().unwrap())
            .collect();

        let leaders: Vec<(usize, u64)> = states.iter().enumerate()
            .filter(|(_, s)| s.role == Role::Leader)
            .map(|(i, s)| (i, s.term))
            .collect();

        let max_term = states.iter().map(|s| s.term).max().unwrap_or(0);

        for (leader_idx, leader_term) in leaders {
            if leader_term < max_term {
                continue;
            }

            let leader_state = &states[leader_idx];
            let leader_log = &leader_state.log;
            let leader_offset = log::offset(leader_log);

            for (i, state) in states.iter().enumerate() {
                let state_offset = log::offset(&state.log);

                for idx in 1..=state.commit_index {
                    // Skip entries below this server's offset (in snapshot)
                    if idx <= state_offset {
                        continue;
                    }

                    let entry = match log::entry_at(&state.log, idx) {
                        Some(e) => e,
                        None => break,
                    };

                    // Leader Completeness: if committed in term T, present in leaders of term >= T.
                    if leader_term >= entry.term {
                        // Entries below leader's offset are covered by snapshot
                        if idx <= leader_offset {
                            continue;
                        }

                        let leader_entry = match log::entry_at(leader_log, idx) {
                            Some(e) => e,
                            None => panic!(
                                "Leader completeness violation: server {} committed index {} (term {}) but leader {} (term {}) doesn't have it",
                                i + 1, idx, entry.term, leader_idx + 1, leader_term
                            ),
                        };
                        assert_eq!(
                            leader_entry.term, entry.term,
                            "Leader completeness violation: server {} committed index {} (term {}) but leader {} (term {}) has term {}",
                            i + 1, idx, entry.term, leader_idx + 1, leader_term, leader_entry.term
                        );
                    }
                }
            }
        }
    }
}
