use serde::{Serialize, Deserialize};
use rand::Rng;

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
    pub nr_clients: u32,
    pub bounce_delay: u32,
}

impl Default for SimConfig {
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
            nr_nodes: 5,
            nr_clients: 3,
            bounce_delay: 5000,
        }
    }
}

impl SimConfig {
    pub fn random() -> Self {
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
            nr_nodes: rng.gen_range(3..=7),
            nr_clients: rng.gen_range(1..=5),
            bounce_delay: rng.gen_range(1000..10000),
        }
    }
}
