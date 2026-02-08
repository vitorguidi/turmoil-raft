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

    /// If two servers have a log entry at the same index, it must have the same term.
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
                    if ei.index == ej.index && ei.term != ej.term {
                        panic!(
                            "Log matching violation at index {}: server {} has term {}, server {} has term {}",
                            ei.index, i + 1, ei.term, j + 1, ej.term
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

        // Find current leader(s)
        let leaders: Vec<usize> = states.iter().enumerate()
            .filter(|(_, s)| s.role == Role::Leader)
            .map(|(i, _)| i)
            .collect();

        if leaders.len() != 1 {
            // No single leader or multiple leaders (checked by election safety), skip
            return;
        }

        let leader = &states[leaders[0]];
        let leader_log_len = leader.log.len();

        for (i, state) in states.iter().enumerate() {
            for idx in 1..=state.commit_index {
                let ui = idx as usize;
                if ui >= state.log.len() {
                    break;
                }
                assert!(
                    ui < leader_log_len,
                    "Leader completeness violation: server {} committed index {} but leader log is too short (len {})",
                    i + 1, idx, leader_log_len
                );
                assert_eq!(
                    leader.log[ui].term, state.log[ui].term,
                    "Leader completeness violation at index {}: leader has term {}, server {} has term {}",
                    idx, leader.log[ui].term, i + 1, state.log[ui].term
                );
            }
        }
    }
}
