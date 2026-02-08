use crate::pb::kv::kv_client::KvClient as TonicKvClient;
use crate::pb::kv::{GetRequest, PutAppendRequest};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

/// Factory that creates fresh tonic channels (same pattern as RaftClient).
pub type ChannelFactory = Arc<dyn Fn() -> Channel + Send + Sync>;

pub struct Clerk {
    servers: Vec<(ChannelFactory, TonicKvClient<Channel>)>,
    leader_id: usize,
    client_id: String,
    seq_num: u64,
    rpc_timeout: Duration,
}

impl Clerk {
    pub fn new(
        factories: Vec<ChannelFactory>,
        client_id: String,
        rpc_timeout: Duration,
    ) -> Self {
        let servers: Vec<_> = factories
            .into_iter()
            .map(|f| {
                let ch = f();
                let client = TonicKvClient::new(ch);
                (f, client)
            })
            .collect();

        Self {
            servers,
            leader_id: 0,
            client_id,
            seq_num: 0,
            rpc_timeout,
        }
    }

    fn reconnect(&mut self, idx: usize) {
        let ch = (self.servers[idx].0)();
        self.servers[idx].1 = TonicKvClient::new(ch);
    }

    pub async fn get(&mut self, key: String) -> String {
        self.seq_num += 1;
        let seq_num = self.seq_num;

        loop {
            let idx = self.leader_id;
            let mut req = tonic::Request::new(GetRequest {
                key: key.clone(),
                client_id: self.client_id.clone(),
                seq_num,
            });
            req.set_timeout(self.rpc_timeout);

            match self.servers[idx].1.get(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if !resp.wrong_leader && resp.err.is_empty() {
                        return resp.value;
                    }
                    if resp.wrong_leader {
                        self.next_leader();
                    }
                }
                Err(status) => {
                    tracing::warn!(server = idx, "Get RPC failed: {}", status);
                    self.reconnect(idx);
                    self.next_leader();
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn put(&mut self, key: String, value: String) {
        self.put_append(key, value, "Put").await;
    }

    pub async fn append(&mut self, key: String, value: String) {
        self.put_append(key, value, "Append").await;
    }

    async fn put_append(&mut self, key: String, value: String, op: &str) {
        self.seq_num += 1;
        let seq_num = self.seq_num;

        loop {
            let idx = self.leader_id;
            let mut req = tonic::Request::new(PutAppendRequest {
                key: key.clone(),
                value: value.clone(),
                op: op.to_string(),
                client_id: self.client_id.clone(),
                seq_num,
            });
            req.set_timeout(self.rpc_timeout);

            match self.servers[idx].1.put_append(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if !resp.wrong_leader && resp.err.is_empty() {
                        return;
                    }
                    if resp.wrong_leader {
                        self.next_leader();
                    }
                }
                Err(status) => {
                    tracing::warn!(server = idx, "PutAppend RPC failed: {}", status);
                    self.reconnect(idx);
                    self.next_leader();
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn next_leader(&mut self) {
        self.leader_id = (self.leader_id + 1) % self.servers.len();
    }
}
