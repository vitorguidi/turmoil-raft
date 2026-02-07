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

    async fn dispatch<Req, Resp>(
        &self,
        request: Request<Req>,
        wrap: impl FnOnce(Req, oneshot::Sender<Resp>) -> RaftMsg,
    ) -> Result<Response<Resp>, Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let msg = wrap(request.into_inner(), reply_tx);
        self.core_tx
            .send(msg)
            .await
            .map_err(|_| Status::internal("Raft core shutdown"))?;
        let resp = reply_rx
            .await
            .map_err(|_| Status::internal("Raft core dropped request"))?;
        Ok(Response::new(resp))
    }
}

#[tonic::async_trait]
impl Raft for RaftService {

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>
    )-> Result<Response<AppendEntriesResponse>, Status> {
        self.dispatch(request, |req, reply| RaftMsg::AppendEntries { req, reply }).await
    }

    async fn request_vote(&self, request: Request<RequestVoteRequest>) -> Result<Response<RequestVoteResponse>, Status> {
        self.dispatch(request, |req: RequestVoteRequest, reply| RaftMsg::RequestVote { req, reply }).await
    }

    async fn install_snapshot(&self, request: Request<InstallSnapshotRequest>) -> Result<Response<InstallSnapshotResponse>, Status> {
        self.dispatch(request, |req: InstallSnapshotRequest, reply| RaftMsg::InstallSnapshot { req, reply }).await
    }

}
