use crate::pb::raft::{raft_server::Raft, AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse, InstallSnapshotRequest, InstallSnapshotResponse};
use tonic::{Request, Response, Status};
use tokio::sync::{mpsc, oneshot};
use crate::raft::core::RaftMsg;

pub struct RaftService{
    core_tx: mpsc::Sender<RaftMsg>,
}

impl RaftService {
    pub fn new(core_tx: mpsc::Sender<RaftMsg>) -> Self {
        Self { core_tx }
    }
}

#[tonic::async_trait]
impl Raft for RaftService {

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>
    )-> Result<Response<AppendEntriesResponse>, Status> {

        let (reply_tx, reply_rx) = oneshot::channel();

        let msg = RaftMsg::AppendEntries {
            req: request.into_inner(),
            reply: reply_tx,
        };

        self.core_tx
            .send(msg)
            .await
            .map_err(|_| Status::internal("Raft core shutdown"))?;

        let resp = reply_rx
            .await
            .map_err(|_| Status::internal("Raft core dropped request"))?;

        Ok(Response::new(resp))

    }

    async fn request_vote(&self, _: Request<RequestVoteRequest>) -> Result<Response<RequestVoteResponse>, Status> {
        unimplemented!()
    }

    async fn install_snapshot(&self, _: Request<InstallSnapshotRequest>) -> Result<Response<InstallSnapshotResponse>, Status> {
        unimplemented!()
    }

}