pub mod oracle;
pub use oracle::*;

pub mod config;
pub use config::*;

pub mod raft_sim;
pub use raft_sim::*;

pub mod kv_sim;
pub use kv_sim::*;

pub mod linearizability_oracle;
pub use linearizability_oracle::*;

use hyper::Uri;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;
use turmoil_raft::raft::core::{RaftState, Role};
use rand::rngs::SmallRng;
use rand::Rng;

pub mod incoming {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tonic::transport::server::{Connected, TcpConnectInfo};
    use turmoil::net::TcpStream;

    pub struct Accepted(pub TcpStream);

    impl Connected for Accepted {
        type ConnectInfo = TcpConnectInfo;

        fn connect_info(&self) -> Self::ConnectInfo {
            Self::ConnectInfo {
                local_addr: self.0.local_addr().ok(),
                remote_addr: self.0.peer_addr().ok(),
            }
        }
    }

    impl AsyncRead for Accepted {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for Accepted {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }
}

pub mod connector {
    use std::{future::Future, pin::Pin};

    use hyper::Uri;
    use hyper_util::rt::TokioIo;

    use tower::Service;
    use turmoil::net::TcpStream;

    type Fut = Pin<Box<dyn Future<Output = Result<TokioIo<TcpStream>, std::io::Error>> + Send>>;

    pub fn connector(
    ) -> impl Service<Uri, Response = TokioIo<TcpStream>, Error = std::io::Error, Future = Fut> + Clone
    {
        tower::service_fn(|uri: Uri| {
            Box::pin(async move {
                let conn = TcpStream::connect(uri.authority().unwrap().as_str()).await?;
                Ok::<_, std::io::Error>(TokioIo::new(conn))
            }) as Fut
        })
    }
}

pub fn create_channel(addr: &str) -> Channel {
    let uri = format!("http://{}", addr).parse::<Uri>().unwrap();
    Endpoint::from(uri)
        // Detect dead connections (e.g. after turmoil partition heals)
        .http2_keep_alive_interval(std::time::Duration::from_millis(500))
        .keep_alive_timeout(std::time::Duration::from_millis(1000))
        .connect_with_connector_lazy(connector::connector())
}

pub fn listener_stream(
    listener: turmoil::net::TcpListener,
) -> ReceiverStream<Result<incoming::Accepted, std::io::Error>> {
    let (tx, rx) = mpsc::channel(128);
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    tracing::info!("Server accepted connection");
                    if tx.send(Ok(incoming::Accepted(stream))).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("Server accept error: {:?}", e);
                    // Continue accepting connections even if one fails
                }
            }
        }
    });
    ReceiverStream::new(rx)
}

pub fn run_sim_loop(
    sim: &mut turmoil::Sim<'static>,
    state_handles: &[Arc<Mutex<RaftState>>],
    oracle: &Oracle,
    config: &SimConfig,
    rng: Arc<Mutex<SmallRng>>,
) -> turmoil::Result {
    let wall_start = std::time::Instant::now();
    let mut down_nodes: BTreeMap<usize, u32> = BTreeMap::new();

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
    let virtual_elapsed = sim.elapsed();
    tracing::info!("--- Simulation Results ---");
    tracing::info!("Physical (Wall) Time: {:?}", wall_elapsed);
    tracing::info!("Virtual (Simulated) Time: {:?}", virtual_elapsed);
    let speedup = virtual_elapsed.as_secs_f64() / wall_elapsed.as_secs_f64();
    tracing::info!("Simulation Speedup: {:.2}x", speedup);

    Ok(())
}

pub async fn run_client_loop(traced: &mut TracedClerk, rng: &mut SmallRng) {
    loop {
        let delay_ms = rng.gen_range(50..500);
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;

        let key = format!("key-{}", rng.gen_range(0..5));
        match rng.gen_range(0..3u32) {
            0 => traced.put(key, format!("v-{}", rng.gen::<u32>())).await,
            1 => traced.append(key, format!("+{}", rng.gen::<u32>())).await,
            2 => { traced.get(key).await; }
            _ => unreachable!(),
        }
    }
}

pub fn run_sim_loop_kv(
    sim: &mut turmoil::Sim<'static>,
    state_handles: &[Arc<Mutex<RaftState>>],
    oracle: &Oracle,
    config: &SimConfig,
    rng: Arc<Mutex<SmallRng>>,
    step_clock: StepClock,
    history: History,
) -> turmoil::Result {
    let mut last_history_len = 0;
    let wall_start = std::time::Instant::now();
    let mut down_nodes: BTreeMap<usize, u32> = BTreeMap::new();

    for step in 0..config.max_steps {
        step_clock.store(step as u64, std::sync::atomic::Ordering::Relaxed);
        sim.step()?;
        oracle.assert_invariants();

        let h = history.lock().unwrap();
        if h.len() > last_history_len {
            check_linearizability(&h, state_handles);
            last_history_len = h.len();
        }
        drop(h);

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
    let virtual_elapsed = sim.elapsed();
    tracing::info!("--- Simulation Results ---");
    tracing::info!("Physical (Wall) Time: {:?}", wall_elapsed);
    tracing::info!("Virtual (Simulated) Time: {:?}", virtual_elapsed);
    let speedup = virtual_elapsed.as_secs_f64() / wall_elapsed.as_secs_f64();
    tracing::info!("Simulation Speedup: {:.2}x", speedup);

    Ok(())
}