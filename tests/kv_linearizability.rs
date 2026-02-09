use turmoil_raft::raft::{
    client::RaftClient,
    core::{Raft, RaftState, Role},
    log,
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
const NR_CLIENTS: u32 = 3;
const HEARTBEAT_INTERVAL: u64 = 150;
const ELECTION_INTERVAL: u64 = 300;
const ELECTION_JITTER: u64 = 150;
const RPC_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_STEPS: u32 = 200_000;
const SEED: u64 = 777;
const OP_DELAY_MIN_MS: u64 = 50;
const OP_DELAY_MAX_MS: u64 = 500;
const CRASH_PROBABILITY: f64 = 0.0003;
const BOUNCE_DELAY_MIN: u64 = 2000;
const BOUNCE_DELAY_MAX: u64 = 6000;

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
                let kv_server = KvServer::new(tx.clone(), apply_rx, persister.clone(), 1000);

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
    let oracle = Oracle::new(state_handles.clone());
    let mut down_nodes: BTreeMap<usize, u64> = BTreeMap::new();
    let mut rng = SmallRng::seed_from_u64(SEED);
    let mut crash_count = 0;
    let mut bounce_count = 0;

    let wall_start = std::time::Instant::now();

    // Spawn concurrent clients on separate hosts
    for c in 0..NR_CLIENTS {
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

        oracle.assert_invariants();

        let h = history.lock().unwrap();
        if h.len() > last_history_len {
            check_linearizability(&h, &state_handles);
            last_history_len = h.len();
        }

        // Bounces
        let bounced: Vec<usize> = down_nodes.iter()
            .filter(|&(_, &bounce_at)| step as u64 >= bounce_at)
            .map(|(&idx, _)| idx)
            .collect();

        for idx in bounced {
            let name = format!("server-{}", idx + 1);
            tracing::info!(step, target = ?name, "BOUNCING");
            sim.bounce(name.as_str());
            down_nodes.remove(&idx);
            bounce_count += 1;
            tracing::info!(step, target = ?name, bounce_count, "BOUNCED");
        }

        // Crashes
        for i in 0..NR_NODES as usize {
            if down_nodes.len() + 1 >= NR_NODES as usize{
                continue;
            }
            if !down_nodes.contains_key(&i) && rng.gen_bool(CRASH_PROBABILITY) {
                let name = format!("server-{}", i + 1);
                tracing::info!(step, target = ?name, "CRASHING");
                sim.crash(name.as_str());
                {
                    let mut s = state_handles[i].lock().unwrap();
                    let snap_offset = log::offset(&s.log);
                    s.commit_index = snap_offset;
                    s.last_applied = snap_offset;
                    s.role = Role::Follower;
                }
                let delay = rng.gen_range(BOUNCE_DELAY_MIN..BOUNCE_DELAY_MAX);
                down_nodes.insert(i, step as u64 + delay);
                crash_count += 1;
                tracing::info!(step, target = ?name, crash_count,"Crashed");
            }
        }

        // Sim updates
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
    

    let wall_elapsed = wall_start.elapsed();
    let virtual_elapsed = sim.elapsed(); // This is the total virtual time passed
    tracing::info!("--- Simulation Results ---");
    tracing::info!("Physical (Wall) Time: {:?}", wall_elapsed);
    tracing::info!("Virtual (Simulated) Time: {:?}", virtual_elapsed);
    
    // High-level systems tip: Calculate the "Speedup Factor"
    let speedup = virtual_elapsed.as_secs_f64() / wall_elapsed.as_secs_f64();
    tracing::info!("Simulation Speedup: {:.2}x", speedup);

    Ok(())
}
