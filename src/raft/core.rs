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

#[derive(Debug, PartialEq)]
pub enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER,
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
    state: RaftState,
    received_votes: u64,
    tx: mpsc::Sender<RaftMsg>,
    rx: mpsc::Receiver<RaftMsg>,
    peers: BTreeMap<u64, RaftClient>,
    rng: Arc<Mutex<SmallRng>>,
    election_timeout: Pin<Box<Sleep>>,
    heartbeat_interval: u64,
    election_interval: u64,
    election_jitter: u64,
    // Persistent state on all servers
    pub term: u64,
    voted_for: Option<u64>,
    log: Vec<LogEntry>,
    // Volatile state on all servers
    commit_index: u64,
    last_applied: u64,
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
    ) -> Self {
        let nr_peers = peers.len();
        Self {
            id,
            state: RaftState::FOLLOWER,
            received_votes: 0,
            tx,
            rx,
            peers,
            rng,
            election_timeout: Box::pin(sleep(Duration::from_millis(election_interval))),
            election_interval,
            election_jitter,
            heartbeat_interval,
            term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
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
        tracing::info!(node = self.id, leader = req.leader_id, term = req.term, "Received AppendEntries");

        if req.term < self.term {
            return AppendEntriesResponse {
                term: self.term,
                ..Default::default()
            };
        }

        self.reset_election_timeout();

        if req.term > self.term || self.state != RaftState::FOLLOWER {
            self.become_follower(req.term);
        }

        // TODO: log consistency check, append entries, advance commit_index
        AppendEntriesResponse {
            term: self.term,
            success: true,
            ..Default::default()
        }
    }

    fn handle_request_vote_req(&mut self, req: RequestVoteRequest) -> RequestVoteResponse {
        if req.term < self.term {
            return RequestVoteResponse {
                term: self.term,
                vote_granted: false,
            };
        }

        if req.term > self.term {
            self.become_follower(req.term);
        }

        let can_vote = self.voted_for.is_none() || self.voted_for == Some(req.candidate_id);

        if can_vote {
            self.voted_for = Some(req.candidate_id);
            self.reset_election_timeout();
            tracing::info!(node = self.id, candidate = req.candidate_id, term = req.term, "Granting vote");
            RequestVoteResponse {
                term: self.term,
                vote_granted: true,
            }
        } else {
            RequestVoteResponse {
                term: self.term,
                vote_granted: false,
            }
        }
    }

    fn handle_install_snapshot_req(&mut self, _: InstallSnapshotRequest) -> InstallSnapshotResponse {
        InstallSnapshotResponse {
            term: self.term,
        }
    }

    // -- election --

    fn handle_request_vote_resp(&mut self, peer_id: u64, resp: RequestVoteResponse) {
        if self.state != RaftState::CANDIDATE {
            return;
        }
        if resp.term > self.term {
            self.become_follower(resp.term);
            self.reset_election_timeout();
            return;
        }
        if resp.vote_granted {
            self.received_votes += 1;
            tracing::info!(node = self.id, peer = peer_id, votes = self.received_votes, quorum = self.quorum(), "Vote granted");
            if self.received_votes >= self.quorum() as u64 {
                self.become_leader();
                self.send_heartbeats();
            }
        }
    }

    fn handle_election_timeout(&mut self) {
        if self.state == RaftState::LEADER {
            return;
        }
        self.become_candidate();
        tracing::info!(node = self.id, term = self.term, "Starting election");

        let req = RequestVoteRequest {
            term: self.term,
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
        if self.state != RaftState::LEADER {
            return;
        }
        self.send_heartbeats();
    }

    fn send_heartbeats(&self) {
        for (&peer_id, peer) in &self.peers {
            let req = AppendEntriesRequest {
                term: self.term,
                leader_id: self.id,
                leader_commit: self.commit_index,
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

    fn become_follower(&mut self, term: u64) {
        tracing::info!(node = self.id, term, "Becoming follower");
        self.state = RaftState::FOLLOWER;
        self.term = term;
        self.voted_for = None;
        self.received_votes = 0;
    }

    fn become_candidate(&mut self) {
        self.state = RaftState::CANDIDATE;
        self.voted_for = Some(self.id);
        self.term += 1;
        self.received_votes = 1;
    }

    fn become_leader(&mut self) {
        tracing::info!(node = self.id, term = self.term, "Became leader");
        self.state = RaftState::LEADER;
    }
}
