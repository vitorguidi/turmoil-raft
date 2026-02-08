use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Server;
use turmoil_raft::pb::raft::raft_client::RaftClient as TonicRaftClient;
use turmoil_raft::pb::raft::raft_server::RaftServer;
use turmoil_raft::raft::{client::RaftClient, core::{Raft, RaftMsg, RaftState, Role}, rpc::RaftService};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::{Arc, Mutex};

mod common;
use common::*;

#[test]
fn ping_test() -> turmoil::Result {
    let _ = tracing_subscriber::fmt().without_time().try_init();
    // oracle checking is O(nr_nodes*|log_size|^2, so lets be considerate
    const MAX_STEPS: u32 = 200000;
    const LATENCY_AVG: f64 = 50.0;
    const ELECTION_JITTER: u32 = 150;
    const ELECTION_INTERVAL: u32 = 300;
    const HEARTBEAT_INTERVAL: u32 = 150;
    const NR_NODES: u32 = 7;


    let rng = Arc::new(Mutex::new(SmallRng::seed_from_u64(42)));

    let rpc_timeout = Duration::from_millis(500);

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(200))
        .enable_random_order()
        // No failure rate for grpc yet
        // https://github.com/tokio-rs/turmoil/issues/185
        //.fail_rate(0.1)
        .build_with_rng(Box::new(SmallRng::seed_from_u64(42)));

    // Exponential distribution with mean = 50ms (lambda = 1/50 = 0.02)
    sim.set_message_latency_curve((1.0 / LATENCY_AVG).into());

    let servers: Vec<_> = (1..=NR_NODES).map(|i| format!("server-{}", i)).collect();
    let mut state_handles = Vec::new();
    let mut tx_handles: Vec<Arc<mpsc::Sender<RaftMsg>>> = Vec::new();

    for (i, server_name) in servers.iter().enumerate() {
        let servers = servers.clone();
        let server_name = server_name.clone();
        let rng = rng.clone();

        // Create shared state for this node
        let state = Arc::new(Mutex::new(RaftState::new()));
        state_handles.push(state.clone());

        // Create channel outside the closure so we can keep a tx handle
        let (tx, rx) = mpsc::channel(100);
        let tx_handle = Arc::new(tx.clone());
        tx_handles.push(tx_handle);

        // Wrap rx in Arc<Mutex> to move into the host closure
        let rx = Arc::new(Mutex::new(Some(rx)));

        sim.host(server_name.as_str(), move || {
            let servers = servers.clone();
            let rng = rng.clone();
            let state = state.clone();
            let tx = tx.clone();
            let rx = rx.clone();
            async move {
                let self_id = i as u64 + 1;
                let rx = rx.lock().unwrap().take().expect("rx already taken");

                // Bind first to ensure we are listening before peers try to connect
                let listener = turmoil::net::TcpListener::bind("0.0.0.0:9000").await?;

                let tx_rpc = tx.clone();
                // Start the gRPC server.
                tokio::spawn(async move {
                    Server::builder()
                        .add_service(RaftServer::new(RaftService::new(tx_rpc)))
                        .serve_with_incoming(listener_stream(listener))
                        .await
                        .unwrap();
                });

                // Create clients for all peers (excluding self).
                let mut peers = BTreeMap::new();
                for (j, peer_name) in servers.iter().enumerate() {
                    let peer_id = j as u64 + 1;
                    if peer_id == self_id {
                        continue;
                    }
                    let channel = create_channel(&format!("{}:9000", peer_name));
                    let tonic_client = TonicRaftClient::new(channel);
                    peers.insert(peer_id, RaftClient::new(tonic_client, rpc_timeout));
                }

                // Run the Raft core directly so the host stays alive
                // (turmoil stops polling spawned tasks once the host closure returns).
                Raft::new(self_id, tx, rx, peers, rng, HEARTBEAT_INTERVAL.into(), ELECTION_INTERVAL.into(), ELECTION_JITTER.into(), state).run().await;

                Ok(())
            }
        });
    }

    // Client host: sleeps random durations, sends Start commands with random content
    {
        let tx_handles = tx_handles.clone();
        let state_handles = state_handles.clone();
        let rng = rng.clone();
        sim.host("client", move || {
            let tx_handles = tx_handles.clone();
            let state_handles = state_handles.clone();
            let rng = rng.clone();
            async move {
                let mut cmd_id = 0u64;
                loop {
                    let sleep_ms = rng.lock().unwrap().gen_range(50..500);
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

                    // Find the leader
                    let leader_idx = state_handles.iter()
                        .position(|s| s.lock().unwrap().role == Role::Leader);

                    if let Some(idx) = leader_idx {
                        cmd_id += 1;
                        let payload_len = rng.lock().unwrap().gen_range(8..64);
                        let payload: Vec<u8> = (0..payload_len)
                            .map(|_| rng.lock().unwrap().gen())
                            .collect();
                        tracing::info!(cmd_id, leader = idx + 1, len = payload.len(), "Client sending Start");
                        let _ = tx_handles[idx].try_send(RaftMsg::Start { command: payload });
                    }
                }
            }
        });
    }

    let oracle = Oracle::new(state_handles.clone());
    let wall_start = std::time::Instant::now();

    // Track active network partitions as (i, j) pairs where i < j
    let mut partitions: std::collections::BTreeSet<(usize, usize)> = std::collections::BTreeSet::new();

    for step in 0..MAX_STEPS {
        // Network chaos: ~1 in 2000 steps, flip a coin
        if rng.lock().unwrap().gen_ratio(1, 2000) {
            let disconnect = rng.lock().unwrap().gen_bool(0.5);
            if disconnect && step < 150000 {
                // Pick a random pair to disconnect
                let a = rng.lock().unwrap().gen_range(0..servers.len());
                let b = (a + rng.lock().unwrap().gen_range(1..servers.len())) % servers.len();
                let pair = (a.min(b), a.max(b));
                if !partitions.contains(&pair) {
                    tracing::info!(step, a = pair.0 + 1, b = pair.1 + 1, "CHAOS: partitioning");
                    sim.partition(servers[pair.0].as_str(), servers[pair.1].as_str());
                    partitions.insert(pair);
                }
            } else if !partitions.is_empty() {
                // Pick a random active partition to repair
                let idx = rng.lock().unwrap().gen_range(0..partitions.len());
                let pair = *partitions.iter().nth(idx).unwrap();
                tracing::info!(step, a = pair.0 + 1, b = pair.1 + 1, "CHAOS: repairing");
                sim.repair(servers[pair.0].as_str(), servers[pair.1].as_str());
                partitions.remove(&pair);
            }
        }

        sim.step()?;
        oracle.assert_invariants();
        if step % 1000 == 0 {
            let commits: Vec<u64> = state_handles.iter()
                .map(|s| s.lock().unwrap().commit_index)
                .collect();
            let log_lens: Vec<usize> = state_handles.iter()
                .map(|s| s.lock().unwrap().log.len())
                .collect();
            let roles: Vec<String> = state_handles.iter()
                .map(|s| format!("{:?}", s.lock().unwrap().role))
                .collect();
            tracing::info!(
                step, ?commits, ?log_lens, ?roles,
                partitions = partitions.len(),
                elapsed = ?std::time::Instant::now().duration_since(wall_start),
                "Simulation progress"
            );
        }
    }

    // Verify all nodes committed at least 5 entries
    for (i, state) in state_handles.iter().enumerate() {
        let s = state.lock().unwrap();
        assert!(
            s.commit_index >= 5,
            "Server {} only committed {} entries (expected >= 5)",
            i + 1,
            s.commit_index,
        );
        assert!(
            s.log.len() >= 6, // sentinel + 5 entries
            "Server {} log len {} (expected >= 6)",
            i + 1,
            s.log.len(),
        );
    }

    Ok(())
}
