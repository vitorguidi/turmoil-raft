use turmoil_raft::raft::{
    client::RaftClient,
    core::{Raft, RaftState},
    persist::Persister,
    rpc::RaftService,
};
use turmoil_raft::kv::{client::Clerk, server::KvServer};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Server;
use turmoil_raft::pb::kv::kv_server::KvServer as TonicKvServer;
use turmoil_raft::pb::raft::raft_server::RaftServer;

use crate::common::*;

pub fn run_kv_simulation(config: SimConfig) -> turmoil::Result {
    let rng = Arc::new(Mutex::new(SmallRng::seed_from_u64(config.seed)));
    let rpc_timeout = Duration::from_millis(500);

    let mut sim = turmoil::Builder::new()
        .enable_random_order()
        .fail_rate(config.network_fail_rate)
        .tcp_capacity(65536)
        .build_with_rng(Box::new(SmallRng::seed_from_u64(config.seed)));

    sim.set_message_latency_curve((1.0 / config.latency_avg).into());

    let servers: Vec<_> = (1..=config.nr_nodes)
        .map(|i| format!("server-{}", i))
        .collect();
    let mut state_handles = Vec::new();
    let mut persister_handles = Vec::new();

    for (i, server_name) in servers.iter().enumerate() {
        let servers = servers.clone();
        let rng = rng.clone();

        let persister = Arc::new(Mutex::new(Persister::new()));
        let state = Arc::new(Mutex::new(RaftState::new()));
        state_handles.push(state.clone());
        persister_handles.push(persister.clone());

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
                let kv_server = KvServer::new(tx.clone(), apply_rx, persister.clone(), config.max_raft_state);

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
                    peers.insert(peer_id, RaftClient::new(factory, rpc_timeout));
                }

                Raft::new(
                    self_id,
                    tx,
                    rx,
                    peers,
                    rng,
                    config.heartbeat_interval.into(),
                    config.election_interval.into(),
                    config.election_jitter.into(),
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

    let step_clock: StepClock = Arc::new(AtomicU64::new(0));
    let history: History = Arc::new(Mutex::new(Vec::new()));
    let oracle = Oracle::new(state_handles.clone());

    // Spawn concurrent clients on separate hosts
    for c in 0..config.nr_clients {
        let step_clock = step_clock.clone();
        let history = history.clone();

        sim.host(format!("client-{}", c).as_str(), move || {
            let step_clock = step_clock.clone();
            let history = history.clone();
            async move {
                let mut rng = SmallRng::seed_from_u64(config.seed + c as u64 + 100);

                let mut factories = Vec::new();
                for i in 1..=config.nr_nodes {
                    let addr = format!("server-{}:9000", i);
                    let factory: Arc<dyn Fn() -> tonic::transport::Channel + Send + Sync> =
                        Arc::new(move || create_channel(&addr));
                    factories.push(factory);
                }

                let client_id = format!("client-{}", c);
                let clerk = Clerk::new(factories, client_id.clone(), rpc_timeout);
                let mut traced = TracedClerk::new(clerk, client_id, step_clock, history);

                run_client_loop(&mut traced, &mut rng).await;
                Ok(())
            }
        });
    }

    run_sim_loop_kv(&mut sim, &state_handles, &oracle, &config, rng, step_clock, history)
}
