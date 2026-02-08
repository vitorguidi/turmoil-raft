mod common;
use common::*;

#[test]
fn raft_deterministic_simulation() -> turmoil::Result {
    let _ = tracing_subscriber::fmt().without_time().try_init();
    run_raft_simulation(SimConfig::default())
}
