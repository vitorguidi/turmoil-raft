## crash-1770646506038.json

vitor@vitor-ubuntu:~/projects/turmoil-raft$ FUZZ_MODE=reproduce FUZZ_FILE=./repro/v22/crash-1770646506038.json TEST_TYPE=kv RUST_LOG=info cargo test --test simulation_harness -- --nocapture > crash.log 2>&1; echo "EXIT: $?"


 INFO simulation_harness::common: --- Simulation Results ---
 INFO simulation_harness::common: Physical (Wall) Time: 150.249758079s
 INFO simulation_harness::common: Virtual (Simulated) Time: 500s
 INFO simulation_harness::common: Simulation Speedup: 3.33x
Reproduction finished without error (flaky test?)
test simulation_harness ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 150.26s


Apparently a flake