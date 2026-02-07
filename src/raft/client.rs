use crate::pb::raft::raft_client::RaftClient as TonicRaftClient;
use crate::pb::raft::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest,
    RequestVoteResponse, InstallSnapshotRequest, InstallSnapshotResponse};
use tonic::transport::Channel;
use tonic::Status;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RaftClient {
    inner: TonicRaftClient<Channel>,
    timeout: Duration,
}

impl RaftClient {
    pub fn new(client: TonicRaftClient<Channel>, timeout: Duration) -> Self {
        Self {
            inner: client,
            timeout,
        }
    }

    async fn invoke<Req, Res, F, Fut>(&self, req: Req, f: F) -> Result<Res, Status>
    where
        F: FnOnce(TonicRaftClient<Channel>, tonic::Request<Req>) -> Fut,
        Fut: std::future::Future<Output = Result<tonic::Response<Res>, Status>>,
    {
        let mut client = self.inner.clone();
        let mut req = tonic::Request::new(req);
        req.set_timeout(self.timeout);
        Ok(f(client, req).await?.into_inner())
    }

    pub async fn request_vote(&self, req: RequestVoteRequest) -> Result<RequestVoteResponse, Status> {
        self.invoke(req, |mut client, req| async move { client.request_vote(req).await }).await
    }

    pub async fn append_entries(&self, req: AppendEntriesRequest) -> Result<AppendEntriesResponse, Status> {
        self.invoke(req, |mut client, req| async move { client.append_entries(req).await }).await
    }

    pub async fn install_snapshot(&self, req: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, Status> {
        self.invoke(req, |mut client, req| async move { client.install_snapshot(req).await }).await
    }
}
