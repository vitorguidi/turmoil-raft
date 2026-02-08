FROM rust:1.84 AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y protobuf-compiler pkg-config libssl-dev

COPY . .

# Build the test binary in release mode
RUN cargo test --test simulation_harness --release --no-run

# Locate and move the binary (using find to avoid grep issues with hex hashes)
RUN find target/release/deps -type f -name "simulation_harness*" ! -name "*.d" -exec cp {} /usr/local/bin/fuzzer \;

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/fuzzer /usr/local/bin/fuzzer

CMD ["/usr/local/bin/fuzzer", "--nocapture"]