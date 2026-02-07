use crate::pb::raft::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};
use tokio::sync::{mpsc, oneshot};

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
}

impl Raft {
    pub fn new(id: u64, rx: mpsc::Receiver<RaftMsg>) -> Self {
        Self {
            id,
            term: 0,
            rx,
        }
    }

    pub async fn run(mut self) {
        let mut election_timer = tokio::time::interval(std::time::Duration::from_millis(300));

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
                }
            }
        }
    }

    pub async fn handle_append_entries (&mut self, _: AppendEntriesRequest) -> AppendEntriesResponse {
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