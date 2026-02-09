use std::collections::{BTreeMap, HashSet};
use crate::pb::raft::{AppendEntriesRequest, AppendEntriesResponse,
     InstallSnapshotRequest, InstallSnapshotResponse, LogEntry,
      RequestVoteRequest, RequestVoteResponse};
use crate::raft::log;
use crate::raft::log::sentinel;
use crate::raft::persist::{Persister, encode, decode};
use rand::Rng;
use tokio::sync::{mpsc, oneshot};
use crate::raft::client::RaftClient;
use tokio::time::{Duration, sleep, Instant, Sleep};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use rand::rngs::SmallRng;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub struct RaftState {
    pub term: u64,
    pub voted_for: Option<u64>,
    pub role: Role,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub debug_log: BTreeMap<u64, Vec<u8>>,
}

impl RaftState {
    pub fn new() -> Self {
        Self {
            term: 0,
            voted_for: None,
            role: Role::Follower,
            log: vec![sentinel()],
            commit_index: 0,
            last_applied: 0,
            debug_log: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub enum RaftMsg {
    AppendEntriesReq {
        req: AppendEntriesRequest,
        reply: oneshot::Sender<AppendEntriesResponse>,
    },
    RequestVoteReq {
        req: RequestVoteRequest,
        reply: oneshot::Sender<RequestVoteResponse>,
    },
    InstallSnapshotReq {
        req: InstallSnapshotRequest,
        reply: oneshot::Sender<InstallSnapshotResponse>,
    },
    RequestVoteResp {
        peer_id: u64,
        resp: RequestVoteResponse,
    },
    AppendEntriesResp {
        peer_id: u64,
        prev_log_index: u64,
        entries_len: u64,
        resp: AppendEntriesResponse,
        heartbeat_id: Option<u64>,
    },
    Start {
        command: Vec<u8>,
        reply: oneshot::Sender<(u64, u64, bool)>, // (index, term, is_leader)
    },
    Snapshot {
        index: u64,
        data: Vec<u8>,
    },
    InstallSnapshotResp {
        peer_id: u64,
        last_included_index: u64,
        resp: InstallSnapshotResponse,
    },
    ReadIndex {
        reply: oneshot::Sender<u64>,
    },
    ElectionTimeout,
    HeartbeatTimeout,
}

/// Messages sent from Raft to the state machine (KV layer) when entries are committed.
#[derive(Debug)]
pub enum ApplyMsg {
    Command { index: u64, term: u64, command: Vec<u8> },
    Snapshot { index: u64, term: u64, data: Vec<u8> },
}

#[derive(Debug)]
pub struct Raft {
    pub id: u64,
    received_votes: u64,
    tx: mpsc::Sender<RaftMsg>,
    rx: mpsc::Receiver<RaftMsg>,
    peers: BTreeMap<u64, RaftClient>,
    rng: Arc<Mutex<SmallRng>>,
    election_timeout: Pin<Box<Sleep>>,
    heartbeat_interval: u64,
    election_interval: u64,
    election_jitter: u64,
    // Shared state for oracle inspection
    pub core: Arc<Mutex<RaftState>>,
    // Persistent state
    persister: Arc<Mutex<Persister>>,
    // Apply channel — delivers committed entries to the state machine (KV layer)
    apply_tx: mpsc::Sender<ApplyMsg>,
    // Volatile leader state (reinitialized on election)
    next_index: BTreeMap<u64, u64>,
    match_index: BTreeMap<u64, u64>,
    // ReadIndex state
    next_heartbeat_id: u64,
    pending_read_batches: BTreeMap<u64, Vec<(u64, oneshot::Sender<u64>)>>,
    heartbeat_acks: BTreeMap<u64, HashSet<u64>>,
    waiting_for_apply: Vec<(u64, oneshot::Sender<u64>)>,
}

impl Raft {
    pub fn new(
        id: u64,
        tx: mpsc::Sender<RaftMsg>,
        rx: mpsc::Receiver<RaftMsg>,
        peers: BTreeMap<u64, RaftClient>,
        rng: Arc<Mutex<SmallRng>>,
        heartbeat_interval: u64,
        election_interval: u64,
        election_jitter: u64,
        core: Arc<Mutex<RaftState>>,
        persister: Arc<Mutex<Persister>>,
        apply_tx: mpsc::Sender<ApplyMsg>,
    ) -> Self {
        {
            let p = persister.lock().unwrap();
            let state_bytes = p.read_raft_state();
            if !state_bytes.is_empty() {
                let (term, voted_for, restored_log, restored_debug_log) = decode(state_bytes);
                let mut c = core.lock().unwrap();
                c.term = term;
                c.voted_for = voted_for;
                c.debug_log = restored_debug_log;
                c.log = restored_log;
                tracing::info!(node=id, term, log_len=c.log.len(), log_offset=log::offset(&c.log), "Restored from persistence");
            }

            // Restore snapshot: set last_applied/commit_index to snapshot boundary
            let snapshot_data = p.read_snapshot();
            if !snapshot_data.is_empty() {
                let mut c = core.lock().unwrap();
                let snap_index = log::offset(&c.log);
                c.commit_index = snap_index;
                c.last_applied = snap_index;
                tracing::info!(node=id, snap_index, "Restoring snapshot on boot");
                apply_tx.try_send(ApplyMsg::Snapshot {
                    index: snap_index,
                    term: c.log[0].term,
                    data: snapshot_data.to_vec(),
                }).expect("apply_tx full on boot");
            }
        }

        Self {
            id,
            received_votes: 0,
            tx,
            rx,
            peers,
            rng,
            election_timeout: Box::pin(sleep(Duration::from_millis(election_interval))),
            election_interval,
            election_jitter,
            heartbeat_interval,
            core,
            persister,
            apply_tx,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            next_heartbeat_id: 0,
            pending_read_batches: BTreeMap::new(),
            heartbeat_acks: BTreeMap::new(),
            waiting_for_apply: Vec::new(),
        }
    }

    fn persist(&self, core: &mut RaftState) {
        let data = encode(core.term, core.voted_for, &core.log, &core.debug_log);
        self.persister.lock().unwrap().save_raft_state(data);
    }

    // -- run loop --

    pub async fn run(mut self) {
        let mut heartbeat_timer = tokio::time::interval(Duration::from_millis(self.heartbeat_interval));
        loop {
            let msg = tokio::select! { biased;
                Some(msg) = self.rx.recv() => msg,
                _ = &mut self.election_timeout => RaftMsg::ElectionTimeout,
                _ = heartbeat_timer.tick() => RaftMsg::HeartbeatTimeout,
            };
            match msg {
                RaftMsg::AppendEntriesReq { req, reply } => {
                    let resp = self.handle_append_entries_req(req);
                    let _ = reply.send(resp);
                },
                RaftMsg::RequestVoteReq { req, reply } => {
                    let resp = self.handle_request_vote_req(req);
                    let _ = reply.send(resp);
                },
                RaftMsg::InstallSnapshotReq { req, reply } => {
                    let resp = self.handle_install_snapshot_req(req).await;
                    let _ = reply.send(resp);
                },
                RaftMsg::RequestVoteResp { peer_id, resp } => {
                    self.handle_request_vote_resp(peer_id, resp);
                },
                RaftMsg::AppendEntriesResp { peer_id, prev_log_index, entries_len, resp, heartbeat_id } => {
                    self.handle_append_entries_resp(peer_id, prev_log_index, entries_len, resp, heartbeat_id);
                },
                RaftMsg::Start { command, reply } => {
                    self.handle_start(command, reply);
                },
                RaftMsg::Snapshot { index, data } => {
                    self.handle_snapshot(index, data);
                },
                RaftMsg::InstallSnapshotResp { peer_id, last_included_index, resp } => {
                    self.handle_install_snapshot_resp(peer_id, last_included_index, resp);
                },
                RaftMsg::ReadIndex { reply } => {
                    self.handle_read_index(reply);
                },
                RaftMsg::ElectionTimeout => {
                    self.handle_election_timeout();
                    self.reset_election_timeout();
                },
                RaftMsg::HeartbeatTimeout => {
                    self.handle_heartbeat_timeout();
                },
            }
            self.apply_committed();
            self.check_read_waiters();
        }
    }

    // -- RPC handlers --

    fn handle_append_entries_req(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        let core_arc = self.core.clone();
        let mut core = core_arc.lock().unwrap();

        if req.term < core.term {
            return AppendEntriesResponse {
                term: core.term,
                ..Default::default()
            };
        }

        self.reset_election_timeout();

        if req.term > core.term || core.role != Role::Follower {
            self.become_follower(&mut core, req.term);
        }

        // Log consistency check (Figure 2)
        if req.prev_log_index > 0 {
            match log::term_at(&core.log, req.prev_log_index) {
                None => {
                    // Our log is too short
                    let conflict_index = log::offset(&core.log) + core.log.len() as u64;
                    tracing::info!(
                        node = self.id, prev_log_index = req.prev_log_index,
                        log_len = core.log.len(), conflict_index,
                        "AppendEntries rejected: log too short"
                    );
                    return AppendEntriesResponse {
                        term: core.term,
                        success: false,
                        conflict_index,
                        conflict_term: 0,
                    };
                }
                Some(t) if t != req.prev_log_term => {
                    // Conflicting term at prev_log_index
                    let conflict_term = t;
                    let conflict_index = log::first_index_for_term(&core.log, conflict_term);
                    tracing::info!(
                        node = self.id, prev_log_index = req.prev_log_index,
                        expected_term = req.prev_log_term, actual_term = t,
                        conflict_index, conflict_term,
                        "AppendEntries rejected: term mismatch"
                    );
                    return AppendEntriesResponse {
                        term: core.term,
                        success: false,
                        conflict_index,
                        conflict_term,
                    };
                }
                _ => {} // Match — proceed
            }
        }

        // Append entries, truncating conflicts
        let mut log_changed = false;
        for entry in &req.entries {
            if entry.index <= log::offset(&core.log) {
                continue; // already in snapshot
            }
            match log::vec_index(&core.log, entry.index) {
                Some(vi) => {
                    if core.log[vi].term != entry.term {
                        // Conflict: truncate from here
                        let conflict_index = log::offset(&core.log) + vi as u64;
                        core.log.truncate(vi);
                        core.debug_log.split_off(&conflict_index);
                        core.log.push(entry.clone());
                        log_changed = true;
                    }
                    // else: already have this entry, skip
                }
                None => {
                    core.log.push(entry.clone());
                    log_changed = true;
                }
            }
            core.debug_log.insert(entry.index, entry.command.clone());
        }

        if log_changed || !req.entries.is_empty() {
            self.persist(&mut core);
        }

        // Advance commit_index
        if req.leader_commit > core.commit_index {
            let last_match_index = req.prev_log_index + req.entries.len() as u64;
            core.commit_index = std::cmp::max(core.commit_index, std::cmp::min(req.leader_commit, last_match_index));
        }

        tracing::info!(
            node = self.id, leader = req.leader_id, term = req.term,
            entries = req.entries.len(), commit = core.commit_index,
            log_len = core.log.len(),
            "AppendEntries accepted"
        );

        AppendEntriesResponse {
            term: core.term,
            success: true,
            ..Default::default()
        }
    }

    fn handle_request_vote_req(&mut self, req: RequestVoteRequest) -> RequestVoteResponse {
        let core_arc = self.core.clone();
        let mut core = core_arc.lock().unwrap();
        if req.term < core.term {
            return RequestVoteResponse {
                term: core.term,
                vote_granted: false,
            };
        }

        if req.term > core.term {
            self.become_follower(&mut core, req.term);
        }

        let can_vote = core.voted_for.is_none() || core.voted_for == Some(req.candidate_id);

        // Election restriction (Section 5.4.1): candidate's log must be
        // at least as up-to-date as ours.
        let our_last_term = log::last_log_term(&core.log);
        let our_last_index = log::last_log_index(&core.log);
        let log_ok = req.last_log_term > our_last_term
            || (req.last_log_term == our_last_term && req.last_log_index >= our_last_index);

        if can_vote && log_ok {
            core.voted_for = Some(req.candidate_id);
            self.persist(&mut core);
            self.reset_election_timeout();
            tracing::info!(node = self.id, candidate = req.candidate_id, term = req.term, "Granting vote");
            RequestVoteResponse {
                term: core.term,
                vote_granted: true,
            }
        } else {
            RequestVoteResponse {
                term: core.term,
                vote_granted: false,
            }
        }
    }

    async fn handle_install_snapshot_req(&mut self, req: InstallSnapshotRequest) -> InstallSnapshotResponse {
        let (term, apply_msg, snapshot_index) = {
            let core_arc = self.core.clone();
            let mut core = core_arc.lock().unwrap();

            if req.term < core.term {
                return InstallSnapshotResponse { term: core.term };
            }

            if req.term > core.term || core.role != Role::Follower {
                self.become_follower(&mut core, req.term);
            }
            self.reset_election_timeout();

            // Already have a snapshot at or past this index
            if req.last_included_index <= log::offset(&core.log) {
                tracing::debug!(
                    node = self.id, req_index = req.last_included_index,
                    our_offset = log::offset(&core.log),
                    "Ignoring InstallSnapshot (already have snapshot at or past this index)"
                );
                return InstallSnapshotResponse { term: core.term };
            }

            tracing::info!(
                node = self.id, term = req.term, leader = req.leader_id,
                last_included_index = req.last_included_index,
                last_included_term = req.last_included_term,
                "Installing snapshot from leader"
            );

            // Prune debug_log to remove potentially stale entries covered by the snapshot
            core.debug_log = core.debug_log.split_off(&(req.last_included_index + 1));

            // Trim log
            let new_sentinel = LogEntry {
                index: req.last_included_index,
                term: req.last_included_term,
                command: vec![],
            };
            if let Some(vi) = log::vec_index(&core.log, req.last_included_index) {
                // We have entries past the snapshot point — keep them
                core.log = core.log.split_off(vi);
                core.log[0] = new_sentinel;
            } else {
                // Our log doesn't extend past the snapshot — reset to just sentinel
                core.log = vec![new_sentinel];
            }

            // Update commit_index.
            // Note: We DO NOT update last_applied yet. We must wait until the snapshot is
            // actually delivered to the state machine to avoid linearizability violations (stale reads).
            if core.commit_index < req.last_included_index {
                core.commit_index = req.last_included_index;
            }

            // Persist raft state + snapshot
            self.persister.lock().unwrap().save_snapshot(req.data.clone());
            self.persist(&mut core);

            (core.term, ApplyMsg::Snapshot {
                index: req.last_included_index,
                term: req.last_included_term,
                data: req.data,
            }, req.last_included_index)
        };

        // Send snapshot to KV applier for state machine restoration
        self.apply_tx.send(apply_msg).await.expect("apply_tx closed");

        // Now that the state machine has the snapshot, we can safely advance last_applied.
        {
            let mut core = self.core.lock().unwrap();
            if core.last_applied < snapshot_index {
                core.last_applied = snapshot_index;
                tracing::info!(node = self.id, last_applied = core.last_applied, "Updated last_applied after snapshot install");
            }
        }

        InstallSnapshotResponse { term }
    }

    /// Handle snapshot from KV server — compact the log.
    fn handle_snapshot(&mut self, index: u64, data: Vec<u8>) {
        let core_arc = self.core.clone();
        let mut core = core_arc.lock().unwrap();

        if index <= log::offset(&core.log) {
            tracing::debug!(node = self.id, index, offset = log::offset(&core.log), "Ignoring snapshot (already compacted past this)");
            return;
        }

        let term = log::entry_at(&core.log, index).map(|e| e.term).unwrap_or(0);
        if term == 0 {
            tracing::warn!(node = self.id, index, "Snapshot index not found in log, ignoring");
            return;
        }

        tracing::info!(
            node = self.id, index, term,
            old_offset = log::offset(&core.log),
            old_log_len = core.log.len(),
            "Compacting log for snapshot"
        );

        let new_sentinel = LogEntry { index, term, command: vec![] };
        if let Some(vi) = log::vec_index(&core.log, index) {
            core.log = core.log.split_off(vi);
            core.log[0] = new_sentinel;
        } else {
            core.log = vec![new_sentinel];
        }

        self.persister.lock().unwrap().save_snapshot(data);
        self.persist(&mut core);

        tracing::info!(
            node = self.id,
            new_offset = log::offset(&core.log),
            new_log_len = core.log.len(),
            "Log compacted"
        );
    }

    /// Handle InstallSnapshot RPC response (leader side).
    fn handle_install_snapshot_resp(&mut self, peer_id: u64, last_included_index: u64, resp: InstallSnapshotResponse) {
        let core_arc = self.core.clone();
        let mut core = core_arc.lock().unwrap();

        if resp.term > core.term {
            self.become_follower(&mut core, resp.term);
            self.reset_election_timeout();
            return;
        }
        if core.role != Role::Leader {
            return;
        }

        self.next_index.insert(peer_id, last_included_index + 1);
        self.match_index.insert(peer_id, last_included_index);
        tracing::info!(
            node = self.id, peer = peer_id,
            next_index = last_included_index + 1,
            match_index = last_included_index,
            "Advanced peer indices after InstallSnapshot"
        );
    }

    // -- election --

    fn handle_request_vote_resp(&mut self, peer_id: u64, resp: RequestVoteResponse) {
        let mut become_leader = false;
        {
            let core_arc = self.core.clone();
            let mut core = core_arc.lock().unwrap();
            if core.role != Role::Candidate {
                return;
            }
            if resp.term > core.term {
                self.become_follower(&mut core, resp.term);
                self.reset_election_timeout();
                return;
            }
            if resp.term < core.term {
                // Stale response from a previous election round; ignore.
                return;
            }
            if resp.vote_granted {
                self.received_votes += 1;
                tracing::info!(node = self.id, peer = peer_id, votes = self.received_votes, quorum = self.quorum(), "Vote granted");
                if self.received_votes >= self.quorum() as u64 {
                    self.become_leader(&mut core);
                    become_leader = true;
                }
            }
        }
        if become_leader {
            self.send_append_entries(None);
        }
    }

    fn handle_append_entries_resp(
        &mut self,
        peer_id: u64,
        prev_log_index: u64,
        entries_len: u64,
        resp: AppendEntriesResponse,
        heartbeat_id: Option<u64>,
    ) {
        let core_arc = self.core.clone();
        let mut core = core_arc.lock().unwrap();
        if resp.term > core.term {
            self.become_follower(&mut core, resp.term);
            self.reset_election_timeout();
            return;
        }
        if core.role != Role::Leader {
            return;
        }

        if resp.success {
            let new_match = prev_log_index + entries_len;
            let current_match = self.match_index.get(&peer_id).copied().unwrap_or(0);
            // Only advance — never go backwards from stale responses
            if new_match > current_match {
                self.match_index.insert(peer_id, new_match);
                self.next_index.insert(peer_id, new_match + 1);
                tracing::info!(
                    node = self.id, peer = peer_id,
                    match_index = new_match, next_index = new_match + 1,
                    "Advanced peer indices"
                );
            }
            let old_commit = core.commit_index;
            self.maybe_advance_commit_index(&mut core);
            if core.commit_index > old_commit {
                tracing::info!(
                    node = self.id,
                    old_commit, new_commit = core.commit_index,
                    "Commit index advanced"
                );
            }

            // Handle ReadIndex heartbeats
            if let Some(hid) = heartbeat_id {
                if let Some(acks) = self.heartbeat_acks.get_mut(&hid) {
                    acks.insert(peer_id);
                    tracing::debug!(node = self.id, heartbeat_id = hid, peer = peer_id, acks = acks.len(), "Received heartbeat ack for ReadIndex");
                    if acks.len() >= self.quorum() {
                        // Quorum reached
                        if let Some(batch) = self.pending_read_batches.remove(&hid) {
                            self.heartbeat_acks.remove(&hid);
                            tracing::info!(node = self.id, heartbeat_id = hid, count = batch.len(), "ReadIndex quorum reached");
                            for (ri, tx) in batch {
                                self.waiting_for_apply.push((ri, tx));
                            }
                        }
                    }
                }
            }

        } else {
            // Fast backup
            let mut new_next_index = resp.conflict_index;
            if resp.conflict_term != 0 {
                if let Some(last_idx) = log::last_index_for_term(&core.log, resp.conflict_term) {
                    new_next_index = last_idx + 1;
                }
            }
            // Sanity check: ensure valid index (>= 1 for real entries, though 1 is start)
            // If conflict_index is 0 (shouldn't happen for active log), clamp to 1.
            if new_next_index < 1 {
                new_next_index = 1;
            }

            self.next_index.insert(peer_id, new_next_index);
            tracing::info!(
                node = self.id, peer = peer_id,
                conflict_term = resp.conflict_term,
                conflict_index = resp.conflict_index,
                new_next_index,
                "AppendEntries rejected, fast backup"
            );
        }
    }

    fn handle_start(&mut self, command: Vec<u8>, reply: oneshot::Sender<(u64, u64, bool)>) {
        let core_arc = self.core.clone();
        let mut core = core_arc.lock().unwrap();
        if core.role != Role::Leader {
            let _ = reply.send((0, 0, false));
            return;
        }
        let index = log::last_log_index(&core.log) + 1;
        let term = core.term;
        core.debug_log.insert(index, command.clone());
        core.log.push(LogEntry {
            index,
            term,
            command,
        });
        self.persist(&mut core);
        tracing::info!(node = self.id, index, term, "Appended new entry");
        let _ = reply.send((index, term, true));
    }

    fn handle_read_index(&mut self, reply: oneshot::Sender<u64>) {
        let core = self.core.lock().unwrap();
        if core.role != Role::Leader {
            // Drop reply to signal error
            tracing::warn!(node = self.id, "ReadIndex rejected: not leader");
            return;
        }
        
        // Ensure we have committed at least one entry in this term (Raft safety for ReadIndex)
        if log::term_at(&core.log, core.commit_index) != Some(core.term) {
             tracing::warn!(node = self.id, commit_index=core.commit_index, term=core.term, "ReadIndex rejected: no committed entry in current term");
             return;
        }
        
        let read_index = core.commit_index;
        let hid = self.next_heartbeat_id;
        self.next_heartbeat_id += 1;
        
        tracing::info!(node = self.id, read_index, heartbeat_id = hid, "Handling ReadIndex");

        self.pending_read_batches.entry(hid).or_default().push((read_index, reply));
        self.heartbeat_acks.entry(hid).or_default().insert(self.id); // Vote for self
        drop(core);
        self.send_append_entries(Some(hid));
    }

    fn apply_committed(&self) {
        let mut core = self.core.lock().unwrap();
        while core.last_applied < core.commit_index {
            let next = core.last_applied + 1;
            let entry = match log::entry_at(&core.log, next) {
                Some(e) => e,
                None => {
                    let offset = log::offset(&core.log);
                    // If we are here, it means we need to apply an entry that is not in our log.
                    // This implies the log was compacted (snapshot) but last_applied wasn't updated,
                    // OR we have a logic bug where commit_index > log.last_index.
                    assert!(next >= offset, "Critical: last_applied ({}) < log offset ({}). Log truncated too early!", core.last_applied, offset);
                    panic!("Entry at index {} not found in log (offset {}, len {}). Commit index is {}.", next, offset, core.log.len(), core.commit_index);
                }
            };
            match self.apply_tx.try_send(ApplyMsg::Command {
                index: entry.index,
                term: entry.term,
                command: entry.command.clone(),
            }) {
                Ok(()) => {
                    core.last_applied = next;
                }
                Err(_) => {
                    tracing::warn!(node = self.id, index = next, "Apply channel full, will retry");
                    break;
                }
            }
        }
    }

    fn check_read_waiters(&mut self) {
        let core = self.core.lock().unwrap();
        let last_applied = core.last_applied;
        drop(core);

        let mut i = 0;
        while i < self.waiting_for_apply.len() {
            if last_applied >= self.waiting_for_apply[i].0 {
                let (ri, tx) = self.waiting_for_apply.remove(i);
                tracing::info!(node = self.id, read_index = ri, last_applied, "ReadIndex satisfied, replying to client");
                let _ = tx.send(ri);
            } else {
                i += 1;
            }
        }
    }

    fn maybe_advance_commit_index(&self, core: &mut RaftState) {
        let last = log::last_log_index(&core.log);
        for n in (core.commit_index + 1)..=last {
            // Only commit entries from the current term (Figure 8 safety)
            let entry_term = log::entry_at(&core.log, n).map(|e| e.term).unwrap_or(0);
            if entry_term != core.term {
                continue;
            }
            // Count replicas: self + peers with match_index >= n
            let mut count = 1u64; // self
            for (_, &mi) in &self.match_index {
                if mi >= n {
                    count += 1;
                }
            }
            if count >= self.quorum() as u64 {
                core.commit_index = n;
                tracing::info!(node=self.id, commit_index=n, "Advanced commit index via quorum");
            }
        }
    }

    fn handle_election_timeout(&mut self) {
        let core_arc = self.core.clone();
        let mut core = core_arc.lock().unwrap();

        if core.role == Role::Leader {
            return;
        }
        self.become_candidate(&mut core);
        let term = core.term;
        let last_log_index = log::last_log_index(&core.log);
        let last_log_term = log::last_log_term(&core.log);
        drop(core);

        tracing::info!(node = self.id, term = term, "Starting election");

        let req = RequestVoteRequest {
            term,
            candidate_id: self.id,
            last_log_index,
            last_log_term,
        };

        for (&peer_id, peer) in &self.peers {
            let mut peer = peer.clone();
            let req = req.clone();
            let tx = self.tx.clone();
            tokio::spawn(async move {
                match peer.request_vote(req).await {
                    Ok(resp) => {
                        let _ = tx.send(RaftMsg::RequestVoteResp { peer_id, resp }).await;
                    },
                    Err(e) => {
                        tracing::warn!(peer = peer_id, "RequestVote RPC failed: {}", e);
                    }
                }
            });
        }
    }

    // -- log replication / heartbeats --

    fn handle_heartbeat_timeout(&mut self) {
        let core = self.core.lock().unwrap();
        if core.role != Role::Leader {
            return;
        }
        drop(core);
        self.send_append_entries(None);
    }

    fn send_append_entries(&self, heartbeat_id: Option<u64>) {
        let core = self.core.lock().unwrap();
        let term = core.term;
        let commit_index = core.commit_index;
        let log_len = core.log.len();
        let log_off = log::offset(&core.log);

        for (&peer_id, peer) in &self.peers {
            let ni = self.next_index.get(&peer_id).copied().unwrap_or(1);

            // Peer is too far behind — send snapshot instead
            if ni <= log_off {
                let snapshot = self.persister.lock().unwrap().read_snapshot().to_vec();
                if snapshot.is_empty() {
                    continue; // no snapshot yet
                }
                let req = InstallSnapshotRequest {
                    term,
                    leader_id: self.id,
                    last_included_index: log_off,
                    last_included_term: core.log[0].term,
                    data: snapshot,
                };
                tracing::info!(
                    node = self.id, peer = peer_id, ni, log_offset = log_off,
                    "Sending InstallSnapshot (peer too far behind)"
                );
                let mut peer = peer.clone();
                let tx = self.tx.clone();
                let last_included_index = log_off;
                tokio::spawn(async move {
                    match peer.install_snapshot(req).await {
                        Ok(resp) => {
                            let _ = tx.send(RaftMsg::InstallSnapshotResp {
                                peer_id,
                                last_included_index,
                                resp,
                            }).await;
                        },
                        Err(e) => {
                            tracing::warn!(peer = peer_id, "InstallSnapshot RPC failed: {}", e);
                        }
                    }
                });
                continue;
            }

            tracing::debug!(
                node = self.id, peer = peer_id, ni, log_len, commit_index,
                "Sending AppendEntries"
            );
            let prev_log_index = ni - 1;
            let prev_log_term = log::entry_at(&core.log, prev_log_index)
                .map(|e| e.term)
                .unwrap_or(0);

            let ni_vec = log::vec_index(&core.log, ni).unwrap_or(core.log.len());
            let entries: Vec<LogEntry> = core.log[ni_vec..].to_vec();
            let entries_len = entries.len() as u64;

            let req = AppendEntriesRequest {
                term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: commit_index,
            };
            if let Some(hid) = heartbeat_id {
                 tracing::debug!(node = self.id, peer = peer_id, heartbeat_id = hid, "Sending AppendEntries with heartbeat_id");
            }

            let mut peer = peer.clone();
            let tx = self.tx.clone();
            tokio::spawn(async move {
                match peer.append_entries(req).await {
                    Ok(resp) => {
                        let _ = tx.send(RaftMsg::AppendEntriesResp {
                            peer_id,
                            prev_log_index,
                            entries_len,
                            resp,
                            heartbeat_id,
                        }).await;
                    },
                    Err(e) => {
                        tracing::warn!(peer = peer_id, "AppendEntries RPC failed: {}", e);
                    }
                }
            });
        }
    }

    // -- helpers --

    fn quorum(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    fn reset_election_timeout(&mut self) {
        let timeout = self.election_interval + self.rng.lock().unwrap().gen_range(0..self.election_jitter);
        self.election_timeout.as_mut().reset(Instant::now() + Duration::from_millis(timeout));
    }

    fn become_follower(&mut self, core: &mut RaftState, term: u64) {
        tracing::info!(node = self.id, term, "Becoming follower");
        core.role = Role::Follower;
        core.term = term;
        core.voted_for = None;
        self.received_votes = 0;
        self.persist(core);
        
        // Clear ReadIndex state
        self.pending_read_batches.clear();
        self.heartbeat_acks.clear();
        self.waiting_for_apply.clear();
    }

    fn become_candidate(&mut self, core: &mut RaftState) {
        core.role = Role::Candidate;
        core.voted_for = Some(self.id);
        core.term += 1;
        self.received_votes = 1;
        self.persist(core);
    }

    fn become_leader(&mut self, core: &mut RaftState) {
        tracing::info!(node = self.id, term = core.term, "Became leader");
        core.role = Role::Leader;

        // Append no-op entry to ensure we can commit entries from previous terms
        // and serve ReadIndex safely.
        let index = log::last_log_index(&core.log) + 1;
        let term = core.term;
        core.debug_log.insert(index, vec![]);
        core.log.push(LogEntry {
            index,
            term,
            command: vec![],
        });
        self.persist(core);
        tracing::info!(node = self.id, index, term, "Appended no-op entry");

        let last = index;
        self.next_index.clear();
        self.match_index.clear();
        for &peer_id in self.peers.keys() {
            self.next_index.insert(peer_id, last + 1);
            self.match_index.insert(peer_id, 0);
        }
        
        // Clear ReadIndex state (sanity)
        self.pending_read_batches.clear();
        self.heartbeat_acks.clear();
        self.waiting_for_apply.clear();
    }
}
