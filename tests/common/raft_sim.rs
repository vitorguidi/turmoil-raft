use turmoil_raft::raft::{client::RaftClient, core::{Raft, RaftMsg, RaftState, Role}, rpc::RaftService, persist::Persister};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Server;
use turmoil_raft::pb::raft::raft_server::RaftServer;

use crate::common::*;

pub fn run_raft_simulation(config: SimConfig) -> turmoil::Result {
    let rng = Arc::new(Mutex::new(SmallRng::seed_from_u64(config.seed)));
    let rpc_timeout = Duration::from_millis(500);

    let mut sim = turmoil::Builder::new()
        .enable_random_order()
        .fail_rate(config.network_fail_rate)
        .tcp_capacity(65536)
        .build_with_rng(Box::new(SmallRng::seed_from_u64(config.seed)));

    sim.set_message_latency_curve((1.0 / config.latency_avg).into());

    let servers: Vec<_> = (1..=config.nr_nodes).map(|i| format!("server-{}", i)).collect();
    let mut state_handles = Vec::new();
    let global_tx_map: Arc<Mutex<BTreeMap<u64, mpsc::Sender<RaftMsg>>>> = Arc::new(Mutex::new(BTreeMap::new()));

    for (i, server_name) in servers.iter().enumerate() {
        let servers = servers.clone();
        let server_name = server_name.clone();
        let rng = rng.clone();
        let global_tx_map = global_tx_map.clone();

        let persister = Arc::new(Mutex::new(Persister::new()));
        let state = Arc::new(Mutex::new(RaftState::new()));
        state_handles.push(state.clone());

        sim.host(server_name.as_str(), move || {
            let servers = servers.clone();
            let rng = rng.clone();
            let state = state.clone();
            let persister = persister.clone();
            let global_tx_map = global_tx_map.clone();
            
            async move {
                let self_id = i as u64 + 1;
                let (tx, rx) = mpsc::channel(100);
                global_tx_map.lock().unwrap().insert(self_id, tx.clone());

                let listener = turmoil::net::TcpListener::bind("0.0.0.0:9000").await?;
                let tx_rpc = tx.clone();
                tokio::spawn(async move {
                    Server::builder()
                        .add_service(RaftServer::new(RaftService::new(tx_rpc)))
                        .serve_with_incoming(listener_stream(listener))
                        .await
                        .unwrap();
                });

                let mut peers = BTreeMap::new();
                for (j, peer_name) in servers.iter().enumerate() {
                    let peer_id = j as u64 + 1;
                    if peer_id == self_id { continue; }
                    let addr = format!("{}:9000", peer_name);
                    let factory: Arc<dyn Fn() -> tonic::transport::Channel + Send + Sync> =
                        Arc::new(move || create_channel(&addr));
                    peers.insert(peer_id, RaftClient::new(factory, rpc_timeout));
                }

                let (apply_tx, _apply_rx) = mpsc::channel(100);
                Raft::new(self_id, tx, rx, peers, rng, config.heartbeat_interval.into(), config.election_interval.into(), config.election_jitter.into(), state, persister, apply_tx).run().await;
                Ok(())
            }
        });
    }

    // Client host
    {
        let global_tx_map = global_tx_map.clone();
        let state_handles = state_handles.clone();
        let rng = rng.clone();
        sim.host("client", move || {
            let global_tx_map = global_tx_map.clone();
            let state_handles = state_handles.clone();
            let rng = rng.clone();
            async move {
                let mut cmd_id = 0u64;
                loop {
                    let sleep_ms = rng.lock().unwrap().gen_range(50..500);
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

                    // Find a leader
                    let leader_idx = state_handles.iter()
                        .position(|s| s.lock().unwrap().role == Role::Leader);

                    if let Some(idx) = leader_idx {
                        cmd_id += 1;
                        let payload: Vec<u8> = vec![(cmd_id % 255) as u8];
                        let leader_id = (idx + 1) as u64;
                        let tx = {
                            let map = global_tx_map.lock().unwrap();
                            map.get(&leader_id).cloned()
                        };
                        if let Some(tx) = tx {
                            // try_send to avoid blocking if channel full
                             let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
                             let _ = tx.try_send(RaftMsg::Start { command: payload, reply: reply_tx });
                        }
                    }
                }
            }
        });
    }

    let oracle = Oracle::new(state_handles.clone());
    run_sim_loop(&mut sim, &state_handles, &oracle, &config, rng)
}
