use crate::pb::raft::raft_client::RaftClient as TonicRaftClient;
use crate::pb::raft::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest,
    RequestVoteResponse, InstallSnapshotRequest, InstallSnapshotResponse};
use tonic::transport::Channel;
use tonic::Status;
use std::sync::Arc;
use std::time::Duration;

/// Factory that creates fresh tonic channels (new TCP connections).
///
/// Tonic's `Channel` multiplexes all RPCs over a single HTTP/2 connection.
/// When the underlying TCP stream is corrupted (e.g. turmoil's `fail_rate`
/// dropping TCP segments, or a network partition killing the connection),
/// the entire HTTP/2 session becomes unusable. Tonic's built-in reconnect
/// (`tower::reconnect::Reconnect`) often fails to recover because the
/// corrupted connection isn't cleanly closed — it's stuck in a zombie state.
///
/// The workaround: on transport errors (`Unavailable`/`Internal`), we replace
/// the channel with a brand-new one from this factory, establishing a fresh
/// TCP connection. We don't retry the failed RPC — Raft's heartbeat cycle
/// (every ~150ms) naturally retries by sending the next AppendEntries or
/// RequestVote on the now-healthy connection.
///
/// See: https://github.com/tokio-rs/turmoil/issues/185
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

    /// Replace the inner channel with a fresh TCP connection.
    fn reconnect(&mut self) {
        let channel = (self.channel_factory)();
        self.inner = TonicRaftClient::new(channel);
    }

    async fn invoke<Req, Res, F, Fut>(&mut self, req: Req, f: F) -> Result<Res, Status>
    where
        F: FnOnce(TonicRaftClient<Channel>, tonic::Request<Req>) -> Fut,
        Fut: std::future::Future<Output = Result<tonic::Response<Res>, Status>>,
    {
        let client = self.inner.clone();
        let mut req = tonic::Request::new(req);
        req.set_timeout(self.timeout);
        match f(client, req).await {
            Ok(resp) => Ok(resp.into_inner()),
            Err(status) => {
                if Self::is_transport_error(&status) {
                    self.reconnect();
                }
                Err(status)
            }
        }
    }

    fn is_transport_error(status: &Status) -> bool {
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
