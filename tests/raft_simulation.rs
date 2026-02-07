use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Server;
use turmoil_raft::pb::raft::raft_client::RaftClient as TonicRaftClient;
use turmoil_raft::pb::raft::raft_server::RaftServer;
use turmoil_raft::raft::{client::RaftClient, core::Raft, rpc::RaftService};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::sync::{Arc, Mutex};

mod common;
use common::*;

#[test]
fn ping_test() -> turmoil::Result {
    let _ = tracing_subscriber::fmt().without_time().try_init();
    const MAX_STEPS: u32 = 10000;

    let rng = Arc::new(Mutex::new(SmallRng::seed_from_u64(42)));

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(200))
        .fail_rate(0.0)
        .enable_random_order()
        .build_with_rng(Box::new(SmallRng::seed_from_u64(42)));

    let servers: Vec<_> = (1..=3).map(|i| format!("server-{}", i)).collect();

    for (i, server_name) in servers.iter().enumerate() {
        let servers = servers.clone();
        let server_name = server_name.clone();
        let rng = rng.clone();
        sim.host(server_name.as_str(), move || {
            let servers = servers.clone();
            let rng = rng.clone();
            async move {
                let self_id = i as u64 + 1;
                let (tx, rx) = mpsc::channel(100);

                // Bind first to ensure we are listening before peers try to connect
                let listener = turmoil::net::TcpListener::bind("0.0.0.0:9000").await?;
                
                // Start the gRPC server.
                tokio::spawn(async move {
                    Server::builder()
                        .add_service(RaftServer::new(RaftService::new(tx)))
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
                    peers.insert(peer_id, RaftClient::new(tonic_client));
                }

                // Run the Raft core directly so the host stays alive
                // (turmoil stops polling spawned tasks once the host closure returns).
                Raft::new(self_id, rx, peers, rng, 150, 300, 150).run().await;

                Ok(())
            }
        });
    }

    // Step the simulation manually since server hosts never return.
    for _ in 0..MAX_STEPS {
        sim.step()?;
    }

    Ok(())
}