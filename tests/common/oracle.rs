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
    }

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
}