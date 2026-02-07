use std::collections::BTreeMap;
use crate::pb::raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, LogEntry, RequestVoteRequest, RequestVoteResponse};
use tokio::sync::{mpsc, oneshot};
use crate::raft::client::RaftClient;

#[derive(Debug)]
pub enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER,
}

#[derive(Debug)]
pub enum RaftMsg {
    AppendEntries {
        req: AppendEntriesRequest,
        reply: oneshot::Sender<AppendEntriesResponse>,
    },
    RequestVote {
        req: RequestVoteRequest,
        reply: oneshot::Sender<RequestVoteResponse>,
    },
    InstallSnapshot {
        req: InstallSnapshotRequest,
        reply: oneshot::Sender<InstallSnapshotResponse>,
    }
}

#[derive(Debug)]
pub struct Raft {
    pub id: u64,
    state: RaftState,
    rx: mpsc::Receiver<RaftMsg>,
    peers: BTreeMap<u64, RaftClient>,
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
    pub fn new(id: u64, rx: mpsc::Receiver<RaftMsg>, peers: BTreeMap<u64, RaftClient>) -> Self {
        let nr_peers = peers.len();
        Self {
            id,
            state: RaftState::FOLLOWER,
            rx,
            peers,
            term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: vec![0; nr_peers + 1],
            match_index: vec![0; nr_peers + 1],
        }
    }

    pub async fn run(mut self) {
        let mut election_timer = tokio::time::interval(std::time::Duration::from_millis(300));
        let mut heartbeat_timer = tokio::time::interval(std::time::Duration::from_millis(150));
        loop {
            tokio::select! { biased;
                Some(msg) = self.rx.recv() => {
                    match msg {
                        RaftMsg::AppendEntries {req, reply} => {
                            let ans = reply.send(self.handle_append_entries(req).await);
                            tracing::info!(node = self.id, "Got append entries response: {:?}", ans);
                        },
                        RaftMsg::RequestVote {req, reply} => {
                            let ans = reply.send(self.handle_request_vote(req).await);
                            tracing::info!(node = self.id, "Got request vote response: {:?}", ans);
                        },
                        RaftMsg::InstallSnapshot {req, reply} => {
                            let ans = reply.send(self.handle_install_snapshot(req).await);
                            tracing::info!(node = self.id, "Got install snapshot response: {:?}", ans);
                        }
                    }
                },
                _ = election_timer.tick() => {
                    self.handle_heartbeat_timeout().await;
                },
                _ = heartbeat_timer.tick() => {
                    self.handle_election_timeout().await;
                }
            }
        }
    }

    pub async fn handle_append_entries (&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        tracing::info!(node = self.id, "Received heartbeat: {:?}", req);
        AppendEntriesResponse {
            ..Default::default()
        }
    }

    pub async fn handle_request_vote(&mut self, _: RequestVoteRequest) -> RequestVoteResponse {
        RequestVoteResponse {
            ..Default::default()
        }
    }

    pub async fn handle_install_snapshot(&mut self, _: InstallSnapshotRequest) -> InstallSnapshotResponse {
        InstallSnapshotResponse {
            term: self.term,
        }
    }

    pub async fn handle_heartbeat_timeout(&mut self) {
        tracing::info!(node = self.id, "Heartbeat timeout");
    }

    pub async fn handle_election_timeout(&mut self) {
        tracing::info!(node = self.id, "Heartbeat timer tick");
        for (&peer_id, peer) in &self.peers {
            let req = AppendEntriesRequest {
                ..Default::default()
            };
            let peer = peer.clone();
            let id = self.id;
            tokio::spawn(async move {
                let resp = peer.append_entries(req).await;
                tracing::info!(node = id, peer = peer_id, "Got append entries response: {:?}", resp);
            });
        }
    }

}
