# CRDT-Based Distributed Exchange Rate Store

> A leaderless, fault-tolerant key-value (kv) store for real-time financial data — built from scratch in Rust.

Live exchange rates from [Frankfurter](https://frankfurter.app) flow continuously into a 3-node cluster where every node accepts writes independently, conflicts resolve automatically using CRDTs, and the system keeps running through node failures without missing a beat.

---

## Why This Exists

Most backend engineers know *how* to use Redis or Postgres. Fewer understand what those tools are actually doing under the hood; what guarantees they provide, where they fall short, and why distributed consistency is hard.

I built this to close that gap for myself.

Phase 1 implements every layer manually: gossip protocol, Write-Ahead Log, vector clocks, CRDT merge functions. Not because reinventing Redis is a good idea in production, but because building it yourself is the only way to truly understand what you're handing off when you do use Redis.

Phase 2 (in progress) replaces the custom infrastructure layer with Redis while keeping the CRDT engine intact — demonstrating the engineering judgment to know *when* to build from scratch and *when* to reach for proven infrastructure.

---

## The Core Problem

Two price feed ingesters fire at the same millisecond. Ingester A writes `USD/EUR = 0.921` to Node A. Ingester B writes `USD/EUR = 0.9215` to Node B. Neither node knows what the other just did.

In a naive replicated system, one write silently overwrites the other based on arbitrary timing. Data is lost. No record of the conflict. No deterministic resolution.

This system handles it differently. Vector clocks detect that the writes were concurrent. The LWW-Register CRDT resolves the conflict deterministically — same result on every node, every time. Within 2 seconds gossip propagates the merged value to all three nodes. No data lost. No human intervention.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Cluster                            │
│                                                         │
│   ┌──────────┐   gossip   ┌──────────┐                 │
│   │  Node A  │◄──────────►│  Node B  │                 │
│   │  :7001   │            │  :7002   │                 │
│   └────┬─────┘            └────┬─────┘                 │
│        │         gossip        │                       │
│        └──────────┬────────────┘                       │
│                   │                                     │
│            ┌──────▼─────┐                              │
│            │   Node C   │                              │
│            │   :7003    │                              │
│            └────────────┘                              │
└─────────────────────────────────────────────────────────┘
         ▲                    ▲
         │                    │
   ┌─────┴──────┐      ┌──────┴─────┐
   │ Frankfurter│      │  CoinCap   │
   │  Ingester  │      │  Ingester  │
   └────────────┘      └────────────┘
         ▲
         │
   ┌─────┴──────┐
   │ kvstore-cli│
   └────────────┘
```

**Every node runs the same binary** — launched three times with different config. There is no leader. Any node accepts reads and writes. Gossip handles the rest.

### Inside Each Node

```
gRPC Server (Tonic)
      │
      ▼
KvStore (DashMap)
      │              │
      ▼              ▼
CRDT Engine        WAL
      │           (disk)
      ▼
Vector Clock
(conflict detection)
      │
      ▼
Gossip Handler
(background sync)
```

---

## How Conflict Resolution Works

Every stored value carries a vector clock — an array of counters, one per node:

```
Node A writes USD/EUR = 0.921   →   clock: [1, 0, 0]
Node B writes USD/EUR = 0.9215  →   clock: [0, 1, 0]
```

Comparing `[1,0,0]` against `[0,1,0]`: Node A is greater in slot 0, Node B in slot 1. Neither dominates. That's a **concurrent write** — a genuine conflict.

The LWW-Register merge function resolves it deterministically using node ID as tiebreak. Both nodes update to the same merged value with clock `[1,1,0]`. Node C catches up on the next gossip round.

All three nodes converge. Always. Mathematically guaranteed.

---

## CRDT Types Implemented

| Type | Stores | Merge Function | Used For |
|------|--------|----------------|----------|
| LWW-Register | Single value | Higher vector clock wins | Exchange rates |
| G-Counter | Counts (up only) | max() per slot | Read counters |
| PN-Counter | Counts (up/down) | max() per slot on both P and N | Write counters |
| OR-Set | Tagged item set | Union preserving tombstones | Tag lists |

---

## Project Structure

```
crdt-exchange-store/
├── node/              
│   └── src/
│       ├── crdt/       
│       ├── gossip/     
│       ├── wal/        
│       ├── store/      
│       ├── vector_clock.rs
│       ├── server.rs  
│       ├── config.rs   
│       └── main.rs     
│
├── cli/                
├── ingester/           
├── shared/             
└── proto/            
```

---

## Running It

**Prerequisites:** Rust, Cargo

**Start the cluster — three terminals:**

```bash
# Node A
NODE_ID=node-a NODE_INDEX=0 PORT=7001 \
PEERS=node-b:localhost:7002,node-c:localhost:7003 \
cargo run -p node

# Node B
NODE_ID=node-b NODE_INDEX=1 PORT=7002 \
PEERS=node-a:localhost:7001,node-c:localhost:7003 \
cargo run -p node

# Node C
NODE_ID=node-c NODE_INDEX=2 PORT=7003 \
PEERS=node-a:localhost:7001,node-b:localhost:7002 \
cargo run -p node
```

**Start the rate ingester:**

```bash
SOURCE=frankfurter TARGET_NODE=http://localhost:7001 \
FETCH_INTERVAL=10 cargo run -p ingester
```

**Query the cluster:**

```bash
# write to Node A
cargo run -p cli -- --node http://localhost:7001 set rates:fiat:USD/EUR 0.921

# read from Node C (different node — eventual consistency in action)
cargo run -p cli -- --node http://localhost:7003 get rates:fiat:USD/EUR

# wait ~2 seconds for gossip, then read again
cargo run -p cli -- --node http://localhost:7003 get rates:fiat:USD/EUR
# rates:fiat:USD/EUR = 0.921
```

---


## Tech Stack

| | |
|---|---|
| **Runtime** | Tokio |
| **gRPC** | Tonic + Prost |
| **Concurrent store** | DashMap |
| **Serialization** | Serde + Bincode |
| **CLI parsing** | Clap |
| **HTTP client** | Reqwest |
| **Logging** | Tracing + tracing-subscriber |

---

## What Phase 2 Adds

- Redis as the storage and replication backbone
- Source priority tiebreaking (Frankfurter > CoinCap for fiat)
- Prometheus metrics + Grafana dashboard
- Atomic Lua scripts for distributed state updates
- Docker Compose for one-command cluster setup
- Full benchmark suite comparing consistency models

---

## Limitations (Phase 1)

This is an honest list. Phase 1 is built for understanding, not production load:

- No authentication or TLS on gRPC connections
- Gossip peer selection is naive (random, not topology-aware)
- No automatic cluster membership — peers are configured statically
- WAL replay on large datasets is slow without index
- Node ID as LWW tiebreak is arbitrary — source priority is better

These are known tradeoffs, not oversights. Phase 2 addresses them systematically.

---


*Phase 1 complete. Phase 2 in progress.*
