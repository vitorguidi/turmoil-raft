use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use turmoil_raft::raft::core::{RaftState, Role};

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
        let mut leaders_per_term = HashMap::new();
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
    pub fn assert_log_matching(&self) {
        let states: Vec<_> = self.handles.iter()
            .map(|h| h.lock().unwrap())
            .collect();

        for i in 0..states.len() {
            for j in (i + 1)..states.len() {
                let min_len = std::cmp::min(states[i].log.len(), states[j].log.len());
                for idx in 0..min_len {
                    let ei = &states[i].log[idx];
                    let ej = &states[j].log[idx];
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
    pub fn assert_state_machine_safety(&self) {
        let states: Vec<_> = self.handles.iter()
            .map(|h| h.lock().unwrap())
            .collect();

        for i in 0..states.len() {
            for j in (i + 1)..states.len() {
                let committed = std::cmp::min(states[i].commit_index, states[j].commit_index);
                for idx in 1..=committed {
                    let ui = idx as usize;
                    if ui >= states[i].log.len() || ui >= states[j].log.len() {
                        break;
                    }
                    let ei = &states[i].log[ui];
                    let ej = &states[j].log[ui];
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
    pub fn assert_leader_completeness(&self) {
        let states: Vec<_> = self.handles.iter()
            .map(|h| h.lock().unwrap())
            .collect();

        // Identify all leaders (active or zombie)
        let leaders: Vec<(usize, u64)> = states.iter().enumerate()
            .filter(|(_, s)| s.role == Role::Leader)
            .map(|(i, s)| (i, s.term))
            .collect();

        let max_leader_term = leaders.iter().map(|(_, t)| *t).max();

        for (leader_idx, leader_term) in leaders {
            if Some(leader_term) != max_leader_term {
                continue;
            }

            let leader_state = &states[leader_idx];
            let leader_log = &leader_state.log;

            for (i, state) in states.iter().enumerate() {
                for idx in 1..=state.commit_index {
                    let ui = idx as usize;
                    if ui >= state.log.len() {
                        break;
                    }
                    let entry = &state.log[ui];

                    // Leader Completeness: if committed in term T, present in leaders of term >= T.
                    if leader_term >= entry.term {
                        assert!(
                            ui < leader_log.len(),
                            "Leader completeness violation: server {} committed index {} (term {}) but leader {} (term {}) log is too short (len {})",
                            i + 1, idx, entry.term, leader_idx + 1, leader_term, leader_log.len()
                        );
                        let leader_entry = &leader_log[ui];
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
