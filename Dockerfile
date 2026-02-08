# ---- Builder Stage ----
# Use Debian Bullseye (older glibc) to ensure compatibility with the runner image
FROM rust:1-bullseye AS builder

# Install dependencies for tonic/prost (protobuf compiler)
RUN apt-get update && apt-get install -y clang protobuf-compiler

WORKDIR /usr/src/turmoil-raft
COPY . .

# Build the test binary in release mode.
# We use --no-run to compile it without executing.
RUN cargo test --test raft_simulation --no-run --release
RUN find target/release/deps -name "raft_simulation-*" -type f -executable -exec cp {} /usr/local/bin/fuzzer \;

# ---- Runner Stage ----
FROM google/cloud-sdk:slim

COPY --from=builder /usr/local/bin/fuzzer /usr/local/bin/fuzzer

WORKDIR /app
ENTRYPOINT ["/usr/local/bin/fuzzer"]
CMD ["raft_fuzz", "--nocapture", "--test-threads=1"]