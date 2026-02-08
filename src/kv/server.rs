use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use crate::pb::kv::kv_server::Kv;
use crate::pb::kv::{GetRequest, GetResponse, PutAppendRequest, PutAppendResponse};
use crate::raft::core::{ApplyMsg, RaftMsg};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KvOp {
    Get {
        key: String,
        client_id: String,
        seq_num: u64,
    },
    Put {
        key: String,
        value: String,
        client_id: String,
        seq_num: u64,
    },
    Append {
        key: String,
        value: String,
        client_id: String,
        seq_num: u64,
    },
}

impl KvOp {
    fn client_id(&self) -> &str {
        match self {
            KvOp::Get { client_id, .. }
            | KvOp::Put { client_id, .. }
            | KvOp::Append { client_id, .. } => client_id,
        }
    }

    fn seq_num(&self) -> u64 {
        match self {
            KvOp::Get { seq_num, .. }
            | KvOp::Put { seq_num, .. }
            | KvOp::Append { seq_num, .. } => *seq_num,
        }
    }
}

/// Result sent back through the pending map when an entry is applied.
#[derive(Debug, Clone)]
pub struct ApplyResult {
    pub value: String,
    pub err: String,
}

/// A pending client request waiting for its Raft entry to be applied.
struct PendingOp {
    term: u64,
    tx: oneshot::Sender<ApplyResult>,
}

pub struct KvServer {
    raft_tx: mpsc::Sender<RaftMsg>,
    pending: Arc<Mutex<HashMap<u64, PendingOp>>>,
    store: Arc<Mutex<HashMap<String, String>>>,
    /// Dedup table: client_id -> (last_seq_num, last_result)
    dup_table: Arc<Mutex<HashMap<String, (u64, String)>>>,
}

impl KvServer {
    pub fn new(
        raft_tx: mpsc::Sender<RaftMsg>,
        apply_rx: mpsc::Receiver<ApplyMsg>,
    ) -> Self {
        let pending: Arc<Mutex<HashMap<u64, PendingOp>>> = Arc::new(Mutex::new(HashMap::new()));
        let store: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
        let dup_table: Arc<Mutex<HashMap<String, (u64, String)>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let server = Self {
            raft_tx,
            pending: pending.clone(),
            store: store.clone(),
            dup_table: dup_table.clone(),
        };

        // Spawn the applier loop
        tokio::spawn(Self::run_applier(apply_rx, pending, store, dup_table));

        server
    }

    async fn run_applier(
        mut apply_rx: mpsc::Receiver<ApplyMsg>,
        pending: Arc<Mutex<HashMap<u64, PendingOp>>>,
        store: Arc<Mutex<HashMap<String, String>>>,
        dup_table: Arc<Mutex<HashMap<String, (u64, String)>>>,
    ) {
        while let Some(msg) = apply_rx.recv().await {
            match msg {
                ApplyMsg::Command {
                    index,
                    term,
                    command,
                } => {
                    let op: KvOp = match bincode::deserialize(&command) {
                        Ok(op) => op,
                        Err(e) => {
                            tracing::warn!(index, "Failed to deserialize KvOp: {}", e);
                            if let Some(pending_op) = pending.lock().unwrap().remove(&index) {
                                let _ = pending_op.tx.send(ApplyResult {
                                    value: String::new(),
                                    err: "deserialization error".to_string(),
                                });
                            }
                            continue;
                        }
                    };

                    tracing::info!(
                        index, term,
                        client_id = %op.client_id(), seq_num = op.seq_num(),
                        op = ?std::mem::discriminant(&op),
                        "Applying committed entry"
                    );

                    let result = Self::apply_op(&op, &store, &dup_table);

                    // Resolve pending request if term matches (same leader that proposed it)
                    if let Some(pending_op) = pending.lock().unwrap().remove(&index) {
                        if pending_op.term == term {
                            tracing::info!(
                                index, term,
                                client_id = %op.client_id(), seq_num = op.seq_num(),
                                "Resolved pending op (term match)"
                            );
                            let _ = pending_op.tx.send(result);
                        } else {
                            tracing::warn!(
                                index, term,
                                pending_term = pending_op.term,
                                client_id = %op.client_id(), seq_num = op.seq_num(),
                                "Dropping pending op (term mismatch — leadership changed)"
                            );
                            // Oneshot dropped → waiting handler gets RecvError → wrong_leader
                        }
                    }
                }
            }
        }
    }

    fn apply_op(
        op: &KvOp,
        store: &Mutex<HashMap<String, String>>,
        dup_table: &Mutex<HashMap<String, (u64, String)>>,
    ) -> ApplyResult {
        let client_id = op.client_id().to_string();
        let seq_num = op.seq_num();

        // Check dedup table
        {
            let dt = dup_table.lock().unwrap();
            if let Some((last_seq, last_result)) = dt.get(&client_id) {
                if seq_num <= *last_seq {
                    tracing::info!(
                        client_id = %client_id, seq_num,
                        last_seq = *last_seq,
                        "Dedup hit — returning cached result"
                    );
                    return ApplyResult {
                        value: last_result.clone(),
                        err: String::new(),
                    };
                }
            }
        }

        let mut s = store.lock().unwrap();
        let result = match op {
            KvOp::Get { key, .. } => {
                let value = s.get(key).cloned().unwrap_or_default();
                tracing::info!(
                    client_id = %client_id, seq_num,
                    key = %key, value = %value,
                    "Applied Get"
                );
                ApplyResult {
                    value,
                    err: String::new(),
                }
            }
            KvOp::Put { key, value, .. } => {
                tracing::info!(
                    client_id = %client_id, seq_num,
                    key = %key, value = %value,
                    "Applied Put"
                );
                s.insert(key.clone(), value.clone());
                ApplyResult {
                    value: String::new(),
                    err: String::new(),
                }
            }
            KvOp::Append { key, value, .. } => {
                let entry = s.entry(key.clone()).or_default();
                entry.push_str(value);
                tracing::info!(
                    client_id = %client_id, seq_num,
                    key = %key, appended = %value, new_value = %entry,
                    "Applied Append"
                );
                ApplyResult {
                    value: String::new(),
                    err: String::new(),
                }
            }
        };

        // Update dedup table
        dup_table
            .lock()
            .unwrap()
            .insert(client_id, (seq_num, result.value.clone()));

        result
    }

    /// Submit a command to Raft and wait for it to be applied.
    async fn submit_and_wait(&self, op: KvOp) -> Result<ApplyResult, Status> {
        let command = bincode::serialize(&op).map_err(|e| {
            Status::internal(format!("serialization error: {}", e))
        })?;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.raft_tx
            .send(RaftMsg::Start {
                command,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Status::unavailable("raft channel closed"))?;

        let (index, term, is_leader) = reply_rx
            .await
            .map_err(|_| Status::unavailable("raft dropped reply"))?;

        if !is_leader {
            return Err(Status::failed_precondition("wrong_leader"));
        }

        // Register pending op
        let (result_tx, result_rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().unwrap();
            pending.insert(index, PendingOp { term, tx: result_tx });
        }

        // Wait with timeout
        match tokio::time::timeout(Duration::from_secs(5), result_rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => {
                // Oneshot dropped — leadership changed, different command at this index
                Err(Status::failed_precondition("wrong_leader"))
            }
            Err(_) => {
                // Timeout — clean up pending entry
                self.pending.lock().unwrap().remove(&index);
                Err(Status::deadline_exceeded("request timed out"))
            }
        }
    }
}

#[tonic::async_trait]
impl Kv for KvServer {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let op = KvOp::Get {
            key: req.key,
            client_id: req.client_id,
            seq_num: req.seq_num,
        };

        match self.submit_and_wait(op).await {
            Ok(result) => Ok(Response::new(GetResponse {
                wrong_leader: false,
                err: result.err,
                value: result.value,
            })),
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                Ok(Response::new(GetResponse {
                    wrong_leader: true,
                    err: String::new(),
                    value: String::new(),
                }))
            }
            Err(status) => Ok(Response::new(GetResponse {
                wrong_leader: false,
                err: status.message().to_string(),
                value: String::new(),
            })),
        }
    }

    async fn put_append(
        &self,
        request: Request<PutAppendRequest>,
    ) -> Result<Response<PutAppendResponse>, Status> {
        let req = request.into_inner();
        let op = match req.op.as_str() {
            "Put" => KvOp::Put {
                key: req.key,
                value: req.value,
                client_id: req.client_id,
                seq_num: req.seq_num,
            },
            "Append" => KvOp::Append {
                key: req.key,
                value: req.value,
                client_id: req.client_id,
                seq_num: req.seq_num,
            },
            other => {
                return Ok(Response::new(PutAppendResponse {
                    wrong_leader: false,
                    err: format!("unknown op: {}", other),
                }));
            }
        };

        match self.submit_and_wait(op).await {
            Ok(result) => Ok(Response::new(PutAppendResponse {
                wrong_leader: false,
                err: result.err,
            })),
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                Ok(Response::new(PutAppendResponse {
                    wrong_leader: true,
                    err: String::new(),
                }))
            }
            Err(status) => Ok(Response::new(PutAppendResponse {
                wrong_leader: false,
                err: status.message().to_string(),
            })),
        }
    }
}
