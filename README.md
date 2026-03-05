# CRDT-Based Distributed Exchange Rate Store

> A leaderless, fault-tolerant key-value store for real-time financial data — built from scratch in Rust.

Live exchange rates from [Frankfurter](https://frankfurter.app) flow continuously into a 3-node cluster where every node accepts writes independently, conflicts resolve automatically using CRDTs, and the system keeps running through node failures without missing a beat.

---

## Why This Exists

Most backend engineers know *how* to use Redis or Postgres. Fewer understand what those tools are actually doing under the hood — what guarantees they provide, where they fall short, and why distributed consistency is hard.

I built this to close that gap for myself.

**Phase 1** implements every layer manually — gossip protocol, Write-Ahead Log, vector clocks, CRDT merge functions. Not because reinventing Redis is a good idea in production, but because building it yourself is the only way to truly understand what you're handing off when you do use Redis.

**Phase 2** replaces the custom infrastructure layer with Redis while keeping the CRDT engine intact — demonstrating the engineering judgment to know *when* to build from scratch and *when* to reach for proven infrastructure.

---

## The Core Problem

Two price feed ingesters fire at the same millisecond. Ingester A writes `USD/EUR = 0.921` to Node A. Ingester B writes `USD/EUR = 0.9215` to Node B. Neither node knows what the other just did.

In a naive replicated system, one write silently overwrites the other based on arbitrary timing. Data is lost. No record of the conflict. No deterministic resolution.

This system handles it differently:

```
Node A writes USD/EUR = 0.921    →   clock: [1, 0, 0]
Node B writes USD/EUR = 0.9215   →   clock: [0, 1, 0]

compare [1,0,0] vs [0,1,0]:
  slot 0: Node A greater
  slot 1: Node B greater
  → neither dominates → Concurrent write detected

LWW-Register merge:
  source priority: frankfurter(3) vs coincap(2)
  frankfurter wins → value = 0.921

merged clock: [max(1,0), max(0,1), max(0,0)] = [1, 1, 0]
```

Both nodes converge to the same value with the same clock. Node C catches up via Pub/Sub within milliseconds. No data lost. No human intervention. Mathematically guaranteed.

---


**Every node runs the same binary** — launched three times with different config. There is no leader. Any node accepts reads and writes.

---

## How Conflict Resolution Works

Every stored value carries a vector clock — an array of counters, one per node:

```
index:  0        1        2
      [Node A,  Node B,  Node C]
```

Each node owns one slot and only ever increments its own.

**Conflict detection:**
```
local    = [2, 0, 0]
incoming = [1, 1, 0]

slot 0: local greater
slot 1: incoming greater
→ Concurrent — neither dominates → merge
```

**Merge — max per slot:**
```
merged clock = [2, 1, 0]
```

**Value tiebreak — source priority:**
```
frankfurter = 3  (most authoritative for fiat)
coincap     = 2
manual      = 1  (CLI writes)
```

---

## CRDT Types Implemented

| Type | Merge | Used For |
|------|-------|----------|
| LWW-Register | Source priority tiebreak | Exchange rates |
| G-Counter | max() per slot | Read counters |
| PN-Counter | max() per slot on inc + dec | Write counters |
| OR-Set | Union preserving tombstone tags | Tag lists |

---

## Phase 1 vs Phase 2

| | Phase 1 | Phase 2 |
|---|---|---|
| Storage | In-memory DashMap | Redis |
| Durability | Custom WAL + snapshots | Redis AOF |
| Sync | Gossip only (2s delay) | Pub/Sub (ms) + Gossip fallback |
| Tiebreak | Node ID alphabetical | Source priority |
| Observability | Atomic counters | Prometheus + /metrics |
| Deployment | Manual terminals | Docker Compose |
| Atomic writes | Mutex | Redis Lua scripts |

The CRDT engine is identical across both phases. Vector clocks, merge functions, conflict resolution — not a line changed.

---

## Project Structure

```
crdt-exchange-store/
├── node/src/
│   ├── crdt/           # CRDT types + merge logic
│   ├── gossip/         # Digest, delta, gossip loop
│   ├── wal/            # Write-Ahead Log + snapshots (Phase 1)
│   ├── store/          # KvStore — central state
│   ├── vector_clock.rs # increment, compare, merge
│   ├── redis_store.rs  # Redis pool + Lua scripts (Phase 2)
│   ├── pubsub.rs       # Redis Pub/Sub subscriber (Phase 2)
│   ├── metrics.rs      # Prometheus counters + histogram
│   ├── server.rs       # gRPC server
│   ├── config.rs       # Node config
│   └── main.rs
├── cli/                # Interactive CLI client
├── ingester/           # Live rate feed
├── shared/             # Common types
└── proto/              # gRPC definitions
```

---

## Running It

**Prerequisites:** Rust, Cargo, Redis

```bash
# start Redis
redis-server

# Node A
NODE_ID=node-a NODE_INDEX=0 PORT=7001 METRICS_PORT=9091 \
PEERS=node-b:localhost:7002,node-c:localhost:7003 \
cargo run -p node

# Node B
NODE_ID=node-b NODE_INDEX=1 PORT=7002 METRICS_PORT=9092 \
PEERS=node-a:localhost:7001,node-c:localhost:7003 \
cargo run -p node

# Node C
NODE_ID=node-c NODE_INDEX=2 PORT=7003 METRICS_PORT=9093 \
PEERS=node-a:localhost:7001,node-b:localhost:7002 \
cargo run -p node

# ingester
SOURCE=frankfurter TARGET_NODE=http://localhost:7001 \
FETCH_INTERVAL=10 cargo run -p ingester

# CLI
cargo run -p cli -- --node http://localhost:7001 set rates:fiat:USD/EUR 0.921
cargo run -p cli -- --node http://localhost:7003 get rates:fiat:USD/EUR

# metrics
curl http://localhost:9091/metrics
```

**Or with Docker:**
```bash
docker-compose up
```

---

## What the Logs Show

```
INFO node::server: SET rates:fiat:USD/EUR = 0.86162    ← write received
INFO node::pubsub: received rate update: rates:fiat:USD/EUR  ← 8ms later on another node
INFO node::gossip: gossip with node-b succeeded         ← safety net running in background
```

Pub/Sub handles real-time propagation in milliseconds. Gossip runs as a fallback to catch anything missed during connection gaps.

---

## Tech Stack

| | |
|---|---|
| **Runtime** | Tokio |
| **gRPC** | Tonic + Prost |
| **Redis** | redis-rs + deadpool-redis |
| **Serialization** | Serde + Bincode |
| **Metrics** | Prometheus |
| **HTTP** | Axum |
| **CLI** | Clap |
| **Logging** | Tracing |

---

## Limitations

- Redis is a single point of failure — production would use Redis Sentinel or Cluster
- No TLS on gRPC connections
- `KEYS rates:*` is blocking — production would use `SCAN`
- Gossip peer selection is naive (random, not topology-aware)
- No automatic cluster membership — peers configured statically

These are known tradeoffs, not oversights.

---

## Background

I built this because I wanted to understand distributed systems from the ground up — not just use the abstractions. 

If you're hiring backend engineers who think carefully about consistency, failure modes, and system design — I'd love to talk.

---

*Phase 1 — custom gossip, WAL, in-memory store — on branch `phase-1`*  
*Phase 2 — Redis backend, Pub/Sub, Prometheus — on branch `phase-2` / `main`*