use std::collections::BTreeMap;
use crate::pb::raft::{AppendEntriesRequest, AppendEntriesResponse,
     InstallSnapshotRequest, InstallSnapshotResponse, LogEntry,
      RequestVoteRequest, RequestVoteResponse};
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
}

impl RaftState {
    pub fn new() -> Self {
        Self {
            term: 0,
            voted_for: None,
            role: Role::Follower,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
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
    ElectionTimeout,
    HeartbeatTimeout,
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
    // Volatile leader state
    next_index: Vec<u64>,
    match_index: Vec<u64>,
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
    ) -> Self {
        let nr_peers = peers.len();
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
            next_index: vec![0; nr_peers + 1],
            match_index: vec![0; nr_peers + 1],
        }
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
                    let resp = self.handle_install_snapshot_req(req);
                    let _ = reply.send(resp);
                },
                RaftMsg::RequestVoteResp { peer_id, resp } => {
                    self.handle_request_vote_resp(peer_id, resp);
                },
                RaftMsg::ElectionTimeout => {
                    self.handle_election_timeout();
                    self.reset_election_timeout();
                },
                RaftMsg::HeartbeatTimeout => {
                    self.handle_heartbeat_timeout();
                },
            }
        }
    }

    // -- RPC handlers --

    fn handle_append_entries_req(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        let core_arc = self.core.clone();
        let mut core = core_arc.lock().unwrap();
        tracing::info!(node = self.id, leader = req.leader_id, term = req.term, "Received AppendEntries");

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

        // TODO: log consistency check, append entries, advance commit_index
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

        if can_vote {
            core.voted_for = Some(req.candidate_id);
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

    fn handle_install_snapshot_req(&mut self, _: InstallSnapshotRequest) -> InstallSnapshotResponse {
        let core = self.core.lock().unwrap();
        InstallSnapshotResponse {
            term: core.term,
        }
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
            self.send_heartbeats();
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
        drop(core); // Drop lock before spawning tasks

        tracing::info!(node = self.id, term = term, "Starting election");

        let req = RequestVoteRequest {
            term,
            candidate_id: self.id,
            ..Default::default()
        };

        for (&peer_id, peer) in &self.peers {
            let peer = peer.clone();
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

    // -- heartbeats --

    fn handle_heartbeat_timeout(&mut self) {
        let core = self.core.lock().unwrap();
        if core.role != Role::Leader {
            return;
        }
        drop(core);
        self.send_heartbeats();
    }

    fn send_heartbeats(&self) {
        let core = self.core.lock().unwrap();
        let term = core.term;
        let commit_index = core.commit_index;
        drop(core);

        for (&peer_id, peer) in &self.peers {
            let req = AppendEntriesRequest {
                term,
                leader_id: self.id,
                leader_commit: commit_index,
                ..Default::default()
            };
            let peer = peer.clone();
            let id = self.id;
            tokio::spawn(async move {
                match peer.append_entries(req).await {
                    Ok(resp) => {
                        tracing::info!(node = id, peer = peer_id, "Heartbeat resp: success={}", resp.success);
                    },
                    Err(e) => {
                        tracing::warn!(node = id, peer = peer_id, "Heartbeat failed: {}", e);
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
    }

    fn become_candidate(&mut self, core: &mut RaftState) {
        core.role = Role::Candidate;
        core.voted_for = Some(self.id);
        core.term += 1;
        self.received_votes = 1;
    }

    fn become_leader(&mut self, core: &mut RaftState) {
        tracing::info!(node = self.id, term = core.term, "Became leader");
        core.role = Role::Leader;
    }
}
