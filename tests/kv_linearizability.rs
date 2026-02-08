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
fn kv_basic_put_get() -> turmoil::Result {
    let _ = tracing_subscriber::fmt().without_time().try_init();

    let (mut sim, _state_handles) = build_kv_sim(42);

    sim.host("client", || async move {
        // Give the cluster time to elect a leader
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut factories = Vec::new();
        for i in 1..=NR_NODES {
            let addr = format!("server-{}:9000", i);
            let factory: Arc<dyn Fn() -> tonic::transport::Channel + Send + Sync> =
                Arc::new(move || create_channel(&addr));
            factories.push(factory);
        }

        let mut clerk = Clerk::new(factories, "client-1".to_string(), RPC_TIMEOUT);

        // Put a value
        clerk.put("key1".to_string(), "value1".to_string()).await;

        // Get the value back
        let val = clerk.get("key1".to_string()).await;
        assert_eq!(val, "value1", "Expected 'value1', got '{}'", val);

        // Overwrite
        clerk.put("key1".to_string(), "value2".to_string()).await;
        let val = clerk.get("key1".to_string()).await;
        assert_eq!(val, "value2");

        // Append
        clerk
            .append("key1".to_string(), "_appended".to_string())
            .await;
        let val = clerk.get("key1".to_string()).await;
        assert_eq!(val, "value2_appended");

        // Get non-existent key
        let val = clerk.get("missing".to_string()).await;
        assert_eq!(val, "");

        tracing::info!("All basic KV assertions passed!");
        Ok(())
    });

    for _ in 0..MAX_STEPS {
        sim.step()?;
    }

    Ok(())
}

#[test]
fn kv_multiple_keys() -> turmoil::Result {
    let _ = tracing_subscriber::fmt().without_time().try_init();

    let (mut sim, _state_handles) = build_kv_sim(123);

    sim.host("client", || async move {
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut factories = Vec::new();
        for i in 1..=NR_NODES {
            let addr = format!("server-{}:9000", i);
            let factory: Arc<dyn Fn() -> tonic::transport::Channel + Send + Sync> =
                Arc::new(move || create_channel(&addr));
            factories.push(factory);
        }

        let mut clerk = Clerk::new(factories, "client-2".to_string(), RPC_TIMEOUT);

        // Write multiple keys
        for i in 0..10 {
            clerk
                .put(format!("key-{}", i), format!("val-{}", i))
                .await;
        }

        // Read them all back
        for i in 0..10 {
            let val = clerk.get(format!("key-{}", i)).await;
            assert_eq!(val, format!("val-{}", i));
        }

        // Append to each
        for i in 0..10 {
            clerk
                .append(format!("key-{}", i), "-extra".to_string())
                .await;
        }

        // Verify appends
        for i in 0..10 {
            let val = clerk.get(format!("key-{}", i)).await;
            assert_eq!(val, format!("val-{}-extra", i));
        }

        tracing::info!("Multiple keys test passed!");
        Ok(())
    });

    for _ in 0..MAX_STEPS {
        sim.step()?;
    }

    Ok(())
}
