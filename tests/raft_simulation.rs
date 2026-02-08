use turmoil_raft::raft::{client::RaftClient, core::{Raft, RaftMsg, RaftState, Role}, rpc::RaftService, persist::Persister};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Server;
use turmoil_raft::pb::raft::raft_server::RaftServer;

mod common;
use common::*;

#[test]
fn raft_deterministic_simulation() -> turmoil::Result {
    let _ = tracing_subscriber::fmt().without_time().try_init();
    
    const MAX_STEPS: u32 = 200000;
    const LATENCY_AVG: f64 = 50.0;
    const NETWORK_FAIL_RATE: f64 = 0.01;
    const ELECTION_JITTER: u32 = 150;
    const ELECTION_INTERVAL: u32 = 300;
    const HEARTBEAT_INTERVAL: u32 = 150;
    const NR_NODES: u32 = 3;
    const RNG_SEED: u64 = 999;
    const CRASH_PROBABILITY: f64 = 1.0 / 20000.0;
    const BOUNCE_DELAY: u32 = 5000;

    let rng = Arc::new(Mutex::new(SmallRng::seed_from_u64(RNG_SEED)));
    let rpc_timeout = Duration::from_millis(500);

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(3600)) // 1 hour virtual time
        .enable_random_order()
        .fail_rate(NETWORK_FAIL_RATE)
        .build_with_rng(Box::new(SmallRng::seed_from_u64(RNG_SEED)));

    sim.set_message_latency_curve((1.0 / LATENCY_AVG).into());

    let servers: Vec<_> = (1..=NR_NODES).map(|i| format!("server-{}", i)).collect();
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

                Raft::new(self_id, tx, rx, peers, rng, HEARTBEAT_INTERVAL.into(), ELECTION_INTERVAL.into(), ELECTION_JITTER.into(), state, persister).run().await;
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
                             let _ = tx.try_send(RaftMsg::Start { command: payload });
                        }
                    }
                }
            }
        });
    }

    let oracle = Oracle::new(state_handles.clone());
    let wall_start = std::time::Instant::now();
    
    // Track nodes that are currently down and when they should return
    let mut down_nodes: BTreeMap<usize, u32> = BTreeMap::new(); // server_idx -> bounce_at_step

    for step in 0..MAX_STEPS {
        sim.step()?;
        oracle.assert_invariants();

        // Check for bounces
        let bounced: Vec<usize> = down_nodes.iter()
            .filter(|&(_, &bounce_at)| step >= bounce_at)
            .map(|(&idx, _)| idx)
            .collect();

        for idx in bounced {
            let name = format!("server-{}", idx + 1);
            tracing::warn!(step, target = ?name, "BOUNCING");
            sim.bounce(name.as_str());
            down_nodes.remove(&idx);
        }

        // Check for crashes (limit to 1 concurrent failure)
        if down_nodes.is_empty() {
             if rng.lock().unwrap().gen_bool(CRASH_PROBABILITY) {
                 let victim_idx = rng.lock().unwrap().gen_range(0..NR_NODES) as usize;
                 let name = format!("server-{}", victim_idx + 1);
                 tracing::warn!(step, target = ?name, "CRASHING");
                 sim.crash(name.as_str());
                 down_nodes.insert(victim_idx, step + BOUNCE_DELAY);
             }
        }

        if step % 1000 == 0 {
             let commits: Vec<u64> = state_handles.iter()
                .map(|s| s.lock().unwrap().commit_index)
                .collect();
             let terms: Vec<u64> = state_handles.iter()
                .map(|s| s.lock().unwrap().term)
                .collect();
             tracing::info!(step, ?commits, ?terms, elapsed = ?std::time::Instant::now().duration_since(wall_start), "Sim step");
        }
    }

    Ok(())
}