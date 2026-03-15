use prometheus::{
    Counter, Histogram, HistogramOpts, IntCounter,
    Registry, TextEncoder, Encoder, opts,
};
use std::sync::OnceLock;
use axum::{routing::get, Router};
use tracing::info;

// ── Global registry ───────────────────────────────────────────
static REGISTRY: OnceLock<Registry> = OnceLock::new();
static READS: OnceLock<IntCounter> = OnceLock::new();
static WRITES: OnceLock<IntCounter> = OnceLock::new();
static GOSSIP_ROUNDS: OnceLock<IntCounter> = OnceLock::new();
static CONFLICTS: OnceLock<IntCounter> = OnceLock::new();
static REQUEST_LATENCY: OnceLock<Histogram> = OnceLock::new();

pub fn init() {
    let registry = Registry::new();

    let reads = IntCounter::with_opts(
        opts!("kvstore_reads_total", "Total read operations")
    ).unwrap();

    let writes = IntCounter::with_opts(
        opts!("kvstore_writes_total", "Total write operations")
    ).unwrap();

    let gossip = IntCounter::with_opts(
        opts!("kvstore_gossip_rounds_total", "Total gossip rounds")
    ).unwrap();

    let conflicts = IntCounter::with_opts(
        opts!("kvstore_conflicts_total", "Total conflicts resolved")
    ).unwrap();

    let latency = Histogram::with_opts(
        HistogramOpts::new("kvstore_request_latency_seconds", "Request latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0])
    ).unwrap();

    registry.register(Box::new(reads.clone())).unwrap();
    registry.register(Box::new(writes.clone())).unwrap();
    registry.register(Box::new(gossip.clone())).unwrap();
    registry.register(Box::new(conflicts.clone())).unwrap();
    registry.register(Box::new(latency.clone())).unwrap();

    REGISTRY.set(registry).ok();
    READS.set(reads).ok();
    WRITES.set(writes).ok();
    GOSSIP_ROUNDS.set(gossip).ok();
    CONFLICTS.set(conflicts).ok();
    REQUEST_LATENCY.set(latency).ok();
}

pub fn inc_reads() {
    if let Some(c) = READS.get() { c.inc(); }
}

pub fn inc_writes() {
    if let Some(c) = WRITES.get() { c.inc(); }
}

pub fn inc_gossip_rounds() {
    if let Some(c) = GOSSIP_ROUNDS.get() { c.inc(); }
}

pub fn inc_conflicts_resolved() {
    if let Some(c) = CONFLICTS.get() { c.inc(); }
}

pub fn observe_latency(seconds: f64) {
    if let Some(h) = REQUEST_LATENCY.get() { h.observe(seconds); }
}

// ── Metrics HTTP server ───────────────────────────────────────
// Prometheus scrapes this endpoint every 15 seconds
pub async fn serve_metrics(port: u16) {
    let app = Router::new().route("/metrics", get(metrics_handler));
    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    info!("metrics server listening on {}", addr);
    axum::serve(listener, app).await.unwrap();
}

async fn metrics_handler() -> String {
    let registry = match REGISTRY.get() {
        Some(r) => r,
        None => return String::new(),
    };

    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}