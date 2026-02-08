use serde_json;

mod common;
use common::*;

#[test]
fn simulation_harness() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .without_time()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")))
        .try_init();

    let bucket = std::env::var("BUCKET_NAME").unwrap_or_else(|_| "raft-crashes".to_string());
    let mode = std::env::var("FUZZ_MODE").unwrap_or_else(|_| "fuzz".to_string());
    let test_type = std::env::var("TEST_TYPE").unwrap_or_else(|_| "raft".to_string());
    let max_steps = std::env::var("MAX_STEPS").ok().and_then(|s| s.parse::<u32>().ok());

    eprintln!("Starting Simulation Harness. Mode: {}, Type: {}", mode, test_type);

    if mode == "reproduce" {
        let file_path = std::env::var("FUZZ_FILE").expect("FUZZ_FILE env var required for reproduce mode");
        eprintln!("Reproducing crash from {}", file_path);
        let data = std::fs::read(file_path).expect("Failed to read fuzz file");
        let config: SimConfig = serde_json::from_slice(&data).expect("Failed to deserialize config");
        eprintln!("Loaded config with seed: {}", config.seed);
        
        let result = match test_type.as_str() {
            "kv" => run_kv_simulation(config),
            _ => run_raft_simulation(config),
        };

        if let Err(e) = result {
            panic!("Reproduction failed (as expected): {:?}", e);
        } else {
            eprintln!("Reproduction finished without error (flaky test?)");
        }
    } else {
        eprintln!("Starting Fuzz Loop...");
        loop {
            let mut config = SimConfig::random();
            if let Some(steps) = max_steps {
                config.max_steps = steps;
            }
            
            let result = std::panic::catch_unwind(|| {
                match test_type.as_str() {
                    "kv" => run_kv_simulation(config.clone()).unwrap(),
                    _ => run_raft_simulation(config.clone()).unwrap(),
                }
            });

            if let Err(e) = result {
                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                    format!("{}", s)
                } else if let Some(s) = e.downcast_ref::<String>() {
                    format!("{}", s)
                } else {
                    "Unknown panic".to_string()
                };
                eprintln!("Test crashed! Panic: {}", msg);
                eprintln!("Saving report...");
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                let filename = format!("crash-{}.json", timestamp);
                let json = serde_json::to_string_pretty(&config).unwrap();
                
                let _ = std::fs::write(&filename, &json);
                eprintln!("Saved locally to {}", filename);

                // Upload to GCS with folder structure: bucket/test_type/filename
                let object_name = format!("{}/{}", test_type, filename);
                eprintln!("Uploading to gs://{}/{}", bucket, object_name);
                common::upload_to_gcs(&bucket, &object_name, &filename);
            }
        }
    }
}
