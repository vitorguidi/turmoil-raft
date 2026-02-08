use turmoil_raft::raft::{client::RaftClient, core::{Raft, RaftMsg, RaftState, Role}, rpc::RaftService, persist::Persister};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Server;
use turmoil_raft::pb::raft::raft_server::RaftServer;
use serde::{Serialize, Deserialize};

mod common;
use common::*;

#[test]
fn raft_deterministic_simulation() -> turmoil::Result {
    let _ = tracing_subscriber::fmt().without_time().try_init();
    run_simulation(SimConfig::default())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SimConfig {
    pub seed: u64,
    pub max_steps: u32,
    pub latency_avg: f64,
    pub network_fail_rate: f64,
    pub crash_probability: f64,
    pub election_jitter: u32,
    pub election_interval: u32,
    pub heartbeat_interval: u32,
    pub nr_nodes: u32,
    pub bounce_delay: u32,
}

impl SimConfig {
    fn default() -> Self {
        let max_steps = std::env::var("MAX_STEPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200_000);
        Self {
            seed: 999,
            max_steps,
            latency_avg: 50.0,
            network_fail_rate: 0.01,
            crash_probability: 1.0 / 20000.0,
            election_jitter: 150,
            election_interval: 300,
            heartbeat_interval: 150,
            nr_nodes: 7,
            bounce_delay: 5000,
        }
    }

    fn random() -> Self {
        let mut rng = rand::thread_rng();
        let election_interval = rng.gen_range(200..800);
        let max_steps = std::env::var("MAX_STEPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| 200_000);
        Self {
            seed: rng.gen(),
            max_steps,
            latency_avg: rng.gen_range(5.0..100.0),
            network_fail_rate: rng.gen_range(0.0..0.05),
            crash_probability: rng.gen_range(0.00001..0.0005),
            election_jitter: rng.gen_range(50..300),
            election_interval,
            heartbeat_interval: election_interval / 3, // Ensure heartbeat << election
            nr_nodes: rng.gen_range(3..=9),
            bounce_delay: rng.gen_range(1000..10000),
        }
    }
}

fn run_simulation(config: SimConfig) -> turmoil::Result {
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

                Raft::new(self_id, tx, rx, peers, rng, config.heartbeat_interval.into(), config.election_interval.into(), config.election_jitter.into(), state, persister).run().await;
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

    for step in 0..config.max_steps {
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
             if rng.lock().unwrap().gen_bool(config.crash_probability) {
                 let victim_idx = rng.lock().unwrap().gen_range(0..config.nr_nodes) as usize;
                 let name = format!("server-{}", victim_idx + 1);
                 tracing::warn!(step, target = ?name, "CRASHING");
                 sim.crash(name.as_str());
                 // Reset volatile state so the oracle doesn't read stale
                 // commit_index / role from the now-dead node.
                 {
                     let mut s = state_handles[victim_idx].lock().unwrap();
                     s.commit_index = 0;
                     s.last_applied = 0;
                     s.role = Role::Follower;
                 }
                 down_nodes.insert(victim_idx, step + config.bounce_delay);
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

#[test]
fn raft_fuzz() {
    // Initialize tracing so we can see logs. try_init() prevents double-init errors.
    let _ = tracing_subscriber::fmt().without_time().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).try_init();

    // Only run if explicitly requested via env var to avoid blocking CI
    if std::env::var("RAFT_FUZZ").is_err() {
        return;
    }

    let bucket = std::env::var("BUCKET_NAME").unwrap_or_else(|_| "raft-crashes".to_string());
    let mode = std::env::var("FUZZ_MODE").unwrap_or_else(|_| "fuzz".to_string());

    if mode == "reproduce" {
        let file_path = std::env::var("FUZZ_FILE").expect("FUZZ_FILE env var required for reproduce mode");
        println!("Reproducing crash from {}", file_path);
        let data = std::fs::read(file_path).expect("Failed to read fuzz file");
        let config: SimConfig = serde_json::from_slice(&data).expect("Failed to deserialize config");
        
        if let Err(e) = run_simulation(config) {
            panic!("Reproduction failed (as expected): {:?}", e);
        } else {
            println!("Reproduction finished without error (flaky test?)");
        }
    } else {
        println!("Starting Fuzz Loop...");
        loop {
            let config = SimConfig::random();
            let blob = serde_json::to_vec_pretty(&config).unwrap();
            
            // Catch panic (Oracle assertion failure) or Result error
            let result = std::panic::catch_unwind(|| {
                run_simulation(config.clone())
            });

            match result {
                Ok(Ok(_)) => { /* Success, continue fuzzing */ }
                Ok(Err(e)) => {
                    eprintln!("Simulation returned error: {:?}", e);
                    upload_crash(&bucket, &blob);
                }
                Err(payload) => {
                    eprintln!("Simulation PANIC: {:?}", payload.downcast_ref::<&str>());
                    upload_crash(&bucket, &blob);
                }
            }
        }
    }
}

fn upload_crash(bucket: &str, blob: &[u8]) {
    let filename = format!("crash-{}.json", rand::thread_rng().gen::<u64>());
    let tmp_path = format!("/tmp/{}", filename);
    std::fs::write(&tmp_path, blob).unwrap();
    
    println!("Uploading crash report {} to gs://{}/crashes/...", filename, bucket);

    // Explicitly authenticate if a key file is provided (fixes 401 error in Docker)
    if let Ok(key_path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        let _ = std::process::Command::new("gcloud")
            .args(&["auth", "activate-service-account", "--key-file", &key_path])
            .output();
    }

    // Use gsutil for simplicity in k8s environment
    let _ = std::process::Command::new("gsutil")
        .args(&["cp", &tmp_path, &format!("gs://{}/crashes/{}", bucket, filename)])
        .status();
}