use turmoil_raft::raft::{
    client::RaftClient,
    core::{Raft, RaftState},
    persist::Persister,
    rpc::RaftService,
};
use turmoil_raft::kv::{client::Clerk, server::KvServer};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Server;
use turmoil_raft::pb::kv::kv_server::KvServer as TonicKvServer;
use turmoil_raft::pb::raft::raft_server::RaftServer;

mod common;
use common::*;

const NR_NODES: u32 = 5;
const HEARTBEAT_INTERVAL: u64 = 150;
const ELECTION_INTERVAL: u64 = 300;
const ELECTION_JITTER: u64 = 150;
const RPC_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_STEPS: u32 = 200_000;
const SEED: u64 = 777;
const OP_DELAY_MIN_MS: u64 = 50;
const OP_DELAY_MAX_MS: u64 = 500;

fn build_kv_sim(seed: u64) -> (turmoil::Sim<'static>, Vec<Arc<Mutex<RaftState>>>) {
    let rng = Arc::new(Mutex::new(SmallRng::seed_from_u64(seed)));

    let mut sim = turmoil::Builder::new()
        .enable_random_order()
        .tcp_capacity(65536)
        .build_with_rng(Box::new(SmallRng::seed_from_u64(seed)));

    let servers: Vec<_> = (1..=NR_NODES)
        .map(|i| format!("server-{}", i))
        .collect();
    let mut state_handles = Vec::new();

    for (i, server_name) in servers.iter().enumerate() {
        let servers = servers.clone();
        let rng = rng.clone();

        let persister = Arc::new(Mutex::new(Persister::new()));
        let state = Arc::new(Mutex::new(RaftState::new()));
        state_handles.push(state.clone());

        let server_name = server_name.clone();
        sim.host(server_name.as_str(), move || {
            let servers = servers.clone();
            let rng = rng.clone();
            let state = state.clone();
            let persister = persister.clone();

            async move {
                let self_id = i as u64 + 1;
                let (tx, rx) = mpsc::channel(100);
                let (apply_tx, apply_rx) = mpsc::channel(100);

                let listener = turmoil::net::TcpListener::bind("0.0.0.0:9000").await?;
                let tx_rpc = tx.clone();

                // Create KV server (spawns applier loop internally)
                let kv_server = KvServer::new(tx.clone(), apply_rx);

                tokio::spawn(async move {
                    Server::builder()
                        .add_service(RaftServer::new(RaftService::new(tx_rpc)))
                        .add_service(TonicKvServer::new(kv_server))
                        .serve_with_incoming(listener_stream(listener))
                        .await
                        .unwrap();
                });

                let mut peers = BTreeMap::new();
                for (j, peer_name) in servers.iter().enumerate() {
                    let peer_id = j as u64 + 1;
                    if peer_id == self_id {
                        continue;
                    }
                    let addr = format!("{}:9000", peer_name);
                    let factory: Arc<dyn Fn() -> tonic::transport::Channel + Send + Sync> =
                        Arc::new(move || create_channel(&addr));
                    peers.insert(peer_id, RaftClient::new(factory, RPC_TIMEOUT));
                }

                Raft::new(
                    self_id,
                    tx,
                    rx,
                    peers,
                    rng,
                    HEARTBEAT_INTERVAL,
                    ELECTION_INTERVAL,
                    ELECTION_JITTER,
                    state,
                    persister,
                    apply_tx,
                )
                .run()
                .await;
                Ok(())
            }
        });
    }

    (sim, state_handles)
}

#[test]
fn kv_linearizability_detsim() -> turmoil::Result {
    let _ = tracing_subscriber::fmt().without_time().try_init();

    let (mut sim, state_handles) = build_kv_sim(SEED);

    let step_clock: StepClock = Arc::new(AtomicU64::new(0));
    let history: History = Arc::new(Mutex::new(Vec::new()));

    // Spawn 3 concurrent clients on separate hosts
    for c in 0..3u32 {
        let step_clock = step_clock.clone();
        let history = history.clone();

        sim.host(format!("client-{}", c).as_str(), move || {
            let step_clock = step_clock.clone();
            let history = history.clone();
            async move {
                let mut rng = SmallRng::seed_from_u64(SEED + c as u64 + 100);

                let mut factories = Vec::new();
                for i in 1..=NR_NODES {
                    let addr = format!("server-{}:9000", i);
                    let factory: Arc<dyn Fn() -> tonic::transport::Channel + Send + Sync> =
                        Arc::new(move || create_channel(&addr));
                    factories.push(factory);
                }

                let client_id = format!("client-{}", c);
                let clerk = Clerk::new(factories, client_id.clone(), RPC_TIMEOUT);
                let mut traced = TracedClerk::new(clerk, client_id, step_clock, history);

                loop {
                    let delay_ms = rng.gen_range(OP_DELAY_MIN_MS..OP_DELAY_MAX_MS);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                    let key = format!("key-{}", rng.gen_range(0..5));
                    match rng.gen_range(0..3u32) {
                        0 => traced.put(key, format!("v-{}-{}", c, rng.gen::<u32>())).await,
                        1 => traced.append(key, format!("+{}", c)).await,
                        2 => { traced.get(key).await; }
                        _ => unreachable!(),
                    }
                }
            }
        });
    }

    let mut last_history_len = 0;
    for step in 0..MAX_STEPS {
        step_clock.store(step as u64, std::sync::atomic::Ordering::Relaxed);
        sim.step()?;

        let h = history.lock().unwrap();
        if h.len() > last_history_len {
            check_linearizability(&h, &state_handles);
            last_history_len = h.len();
        }
    }

    Ok(())
}
