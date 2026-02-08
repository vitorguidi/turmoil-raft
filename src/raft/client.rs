use crate::pb::raft::raft_client::RaftClient as TonicRaftClient;
use crate::pb::raft::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest,
    RequestVoteResponse, InstallSnapshotRequest, InstallSnapshotResponse};
use tonic::transport::Channel;
use tonic::Status;
use std::sync::Arc;
use std::time::Duration;

/// Factory that creates fresh tonic channels (new TCP connections).
pub type ChannelFactory = Arc<dyn Fn() -> Channel + Send + Sync>;

#[derive(Clone)]
pub struct RaftClient {
    inner: TonicRaftClient<Channel>,
    channel_factory: ChannelFactory,
    timeout: Duration,
}

impl std::fmt::Debug for RaftClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftClient")
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl RaftClient {
    pub fn new(channel_factory: ChannelFactory, timeout: Duration) -> Self {
        let channel = channel_factory();
        Self {
            inner: TonicRaftClient::new(channel),
            channel_factory,
            timeout,
        }
    }

    const MAX_RETRIES: u32 = 3;
    const RETRY_DELAY_MS: u64 = 50;

    /// Reconnect: create a fresh channel and tonic client.
    fn reconnect(&mut self) {
        let channel = (self.channel_factory)();
        self.inner = TonicRaftClient::new(channel);
    }

    async fn invoke<Req, Res, F, Fut>(&mut self, req: Req, f: F) -> Result<Res, Status>
    where
        Req: Clone,
        F: Fn(TonicRaftClient<Channel>, tonic::Request<Req>) -> Fut,
        Fut: std::future::Future<Output = Result<tonic::Response<Res>, Status>>,
    {
        let mut last_err = Status::internal("no attempts made");
        for attempt in 0..=Self::MAX_RETRIES {
            let client = self.inner.clone();
            let mut req = tonic::Request::new(req.clone());
            req.set_timeout(self.timeout);
            match f(client, req).await {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(status) if Self::is_retryable(&status) => {
                    last_err = status;
                    // Create a completely fresh channel to bypass the broken HTTP/2 connection
                    self.reconnect();
                    if attempt < Self::MAX_RETRIES {
                        tokio::time::sleep(Duration::from_millis(Self::RETRY_DELAY_MS)).await;
                    }
                }
                Err(status) => return Err(status),
            }
        }
        Err(last_err)
    }

    fn is_retryable(status: &Status) -> bool {
        matches!(
            status.code(),
            tonic::Code::Unavailable | tonic::Code::Internal
        )
    }

    pub async fn request_vote(&mut self, req: RequestVoteRequest) -> Result<RequestVoteResponse, Status> {
        self.invoke(req, |mut client, req| async move { client.request_vote(req).await }).await
    }

    pub async fn append_entries(&mut self, req: AppendEntriesRequest) -> Result<AppendEntriesResponse, Status> {
        self.invoke(req, |mut client, req| async move { client.append_entries(req).await }).await
    }

    pub async fn install_snapshot(&mut self, req: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, Status> {
        self.invoke(req, |mut client, req| async move { client.install_snapshot(req).await }).await
    }
}
