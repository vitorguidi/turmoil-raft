use crate::pb::raft::LogEntry;

/// The log uses a sentinel at index 0 (term=0, empty command).
/// Real entries start at index 1.
///
/// After snapshot compaction, log[0] becomes a sentinel with
/// index = last_included_index, term = last_included_term.
/// Use offset() / vec_index() / entry_at() for all access.

/// First global index in the log (sentinel's index = last_included_index).
pub fn offset(log: &[LogEntry]) -> u64 {
    log[0].index
}

/// Convert global Raft index to vec position. None if out of range.
pub fn vec_index(log: &[LogEntry], global_index: u64) -> Option<usize> {
    let off = offset(log);
    if global_index < off {
        return None;
    }
    let i = (global_index - off) as usize;
    if i < log.len() {
        Some(i)
    } else {
        None
    }
}

/// Get entry at global index.
pub fn entry_at(log: &[LogEntry], global_index: u64) -> Option<&LogEntry> {
    vec_index(log, global_index).map(|i| &log[i])
}

pub fn last_log_index(log: &[LogEntry]) -> u64 {
    log.last().expect("log must have sentinel").index
}

pub fn last_log_term(log: &[LogEntry]) -> u64 {
    log.last().expect("log must have sentinel").term
}

pub fn term_at(log: &[LogEntry], index: u64) -> Option<u64> {
    vec_index(log, index).map(|i| log[i].term)
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
