use crate::pb::raft::LogEntry;

/// The log uses a sentinel at index 0 (term=0, empty command).
/// Real entries start at index 1.

pub fn last_log_index(log: &[LogEntry]) -> u64 {
    log.last().expect("log must have sentinel").index
}

pub fn last_log_term(log: &[LogEntry]) -> u64 {
    log.last().expect("log must have sentinel").term
}

pub fn term_at(log: &[LogEntry], index: u64) -> Option<u64> {
    let idx = index as usize;
    if idx < log.len() && log[idx].index == index {
        Some(log[idx].term)
    } else {
        None
    }
}

/// Find the first index in the log with the given term.
pub fn first_index_for_term(log: &[LogEntry], term: u64) -> u64 {
    for entry in log {
        if entry.term == term {
            return entry.index;
        }
    }
    0
}

/// Find the last index in the log with the given term.
pub fn last_index_for_term(log: &[LogEntry], term: u64) -> Option<u64> {
    for entry in log.iter().rev() {
        if entry.term == term {
            return Some(entry.index);
        }
    }
    None
}

pub fn sentinel() -> LogEntry {
    LogEntry {
        index: 0,
        term: 0,
        command: vec![],
    }
}
