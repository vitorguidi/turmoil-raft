use crate::pb::raft::raft_client::RaftClient as TonicRaftClient;
use crate::pb::raft::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest,
    RequestVoteResponse, InstallSnapshotRequest, InstallSnapshotResponse};
use tonic::transport::Channel;
use tonic::Status;

#[derive(Clone, Debug)]
pub struct RaftClient {
    inner: TonicRaftClient<Channel>,
}

impl RaftClient {
    pub fn new(client: TonicRaftClient<Channel>) -> Self {
        Self {
            inner: client,
        }
    }

    pub async fn request_vote(&self, req: RequestVoteRequest) -> Result<RequestVoteResponse, Status> {
        let mut client = self.inner.clone();
        Ok(client.request_vote(req).await?.into_inner())
    }

    pub async fn append_entries(&self, req: AppendEntriesRequest) -> Result<AppendEntriesResponse, Status> {
        let mut client = self.inner.clone();
        Ok(client.append_entries(req).await?.into_inner())
    }

    pub async fn install_snapshot(&self, req: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, Status> {
        let mut client = self.inner.clone();
        Ok(client.install_snapshot(req).await?.into_inner())
    }
}
