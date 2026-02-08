# Raft Simulation & Fuzzing

This directory contains deterministic simulation tests for the Raft implementation using `turmoil`. It includes a fuzzer that can run continuously to find edge cases (e.g., leader safety violations).

## 1. Build the Fuzzer Docker Image

From the project root:

```bash
# Build the image (uses a multi-stage build to keep it small)
docker build -t turmoil-raft-fuzzer .
```

## 2. Run Fuzzing in Docker

To run the fuzzer locally (simulating the Kubernetes deployment):

```bash
docker run --rm -it \
  -e RAFT_FUZZ=1 \
  -e FUZZ_MODE=fuzz \
  -e MAX_STEPS=100000 \
  -e BUCKET_NAME=your-bucket-name \
  -e GOOGLE_APPLICATION_CREDENTIALS=/gcp/sa.json \
  -v /path/to/your/service-account.json:/gcp/sa.json:ro \
  turmoil-raft-fuzzer
```

*   `MAX_STEPS`: Number of simulation steps per run (default 1M).
*   `BUCKET_NAME`: GCS bucket to upload crash reports to.
*   `GOOGLE_APPLICATION_CREDENTIALS`: Path inside container to GCP key.
*   `-v ...`: Mounts your local GCP service account key into the container.

If you don't have GCP credentials set up locally, the fuzzer will still run and print panics to stdout, but the upload step will fail.

## 3. Reproduce a Crash with Logs

If the fuzzer finds a crash, it saves a JSON configuration file (e.g., `crash-12345.json`). To reproduce the exact sequence of events locally with logging enabled:

1.  Save the crash configuration to a file (e.g., `repro.json`).
2.  Run the test locally using `cargo`:

```bash
# Set RUST_LOG to see details (info, debug, trace)
RUST_LOG=info RAFT_FUZZ=1 FUZZ_MODE=reproduce FUZZ_FILE=repro.json \
    cargo test --test raft_simulation raft_fuzz -- --nocapture
```

This will run the simulation with the exact seed and parameters from the crash file, allowing you to debug the failure.

## 4. Deploy to Kubernetes

To run the fuzzer at scale on a Kubernetes cluster, use the template in `k8s/`.

1.  **Build and Push Image:**
    ```bash
    docker build -t gcr.io/your-project/turmoil-raft-fuzzer:latest .
    docker push gcr.io/your-project/turmoil-raft-fuzzer:latest
    ```

2.  **Deploy:**
    Set the required environment variables and apply the template using `envsubst`.

    ```bash
    export FUZZER_IMAGE=gcr.io/your-project/turmoil-raft-fuzzer:latest
    export BUCKET_NAME=your-crash-bucket
    export GCP_KEY_BASE64=$(cat /path/to/sa.json | base64 | tr -d '\n')

    envsubst < k8s/k8s-deployment.yaml.tmpl | kubectl apply -f -
    ```
