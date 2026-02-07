use std::collections::BTreeMap;
use crate::{pb::raft::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse}};
use tokio::sync::{mpsc, oneshot};
use crate::raft::client::RaftClient;

pub enum RaftMsg {
    AppendEntries {
        req: AppendEntriesRequest,
        reply: oneshot::Sender<AppendEntriesResponse>,
    }, RequestVote {
        req: RequestVoteRequest,
        reply: oneshot::Sender<RequestVoteResponse>,
    }
}

pub struct Raft {
    pub id: u64,
    pub term: u64,
    rx: mpsc::Receiver<RaftMsg>,
    peers: BTreeMap<u64, RaftClient>,
}

impl Raft {
    pub fn new(id: u64, rx: mpsc::Receiver<RaftMsg>, peers: BTreeMap<u64, RaftClient>) -> Self {
        Self {
            id,
            term: 0,
            rx,
            peers,
        }
    }

    pub async fn run(mut self) {
        let mut election_timer = tokio::time::interval(std::time::Duration::from_millis(300));
        let mut heartbeat_timer = tokio::time::interval(std::time::Duration::from_millis(150));
        loop {
            tokio::select! {
                Some(msg) = self.rx.recv() => {
                    match msg {
                        RaftMsg::AppendEntries {req, reply} => {
                            let ans = reply.send(self.handle_append_entries(req).await);
                            tracing::info!(node = self.id, "Got append entries response: {:?}", ans);
                        },
                        RaftMsg::RequestVote {req, reply} => {
                            let ans = reply.send(self.handle_request_vote(req).await);
                            tracing::info!(node = self.id, "Got request vote response: {:?}", ans);
                        }
                    }
                },
                _ = election_timer.tick() => {
                    tracing::info!(node = self.id, "Election timer tick");
                },
                _ = heartbeat_timer.tick() => {
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

}