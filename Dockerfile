FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release -p node

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/node /usr/local/bin/node
CMD ["node"]