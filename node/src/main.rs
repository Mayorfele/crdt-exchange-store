mod vector_clock;
mod crdt;
mod wal;
mod store;
mod gossip;
mod config;
mod metrics;
mod server;

use std::sync::{Arc, Mutex};
use std::fs;
use clap::Parser;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;
use kvstore_proto::kvstore::kv_store_server::KvStoreServer;
use crate::config::Config;
use crate::store::KvStore;
use crate::server::NodeService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Logging ──────────────────────────────────────────────
    // reads LOG_LEVEL env var. defaults to "info"
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("info".parse()?))
        .init();

    // ── Config ───────────────────────────────────────────────
    let config = Config::parse();
    info!(
        "starting node {} on port {} with {} peers",
        config.node_id,
        config.port,
        config.peers().len()
    );

    // ── Data directory ───────────────────────────────────────
    // create the data dir if it doesn't exist yet
    fs::create_dir_all(&config.data_dir)?;

    // ── Store ────────────────────────────────────────────────
    // boot the store — loads snapshot + replays WAL
    let store = KvStore::new(
        config.node_id.clone(),
        config.node_index,
        config.num_nodes,
        config.wal_path(),
        config.snapshot_path(),
    )?;

    // wrap in Arc<Mutex<>> so it can be shared across
    // the gRPC server and the gossip background task
    let store = Arc::new(Mutex::new(store));

    // ── Gossip background task ───────────────────────────────
    // spawn gossip as a separate async task so it runs
    // independently of the gRPC server
    let gossip_store = store.clone();
    let peers = config.peers();
    let gossip_interval = config.gossip_interval;

    tokio::spawn(async move {
        gossip::run_gossip(gossip_store, peers, gossip_interval).await;
    });

    // ── gRPC server ──────────────────────────────────────────
    let addr = format!("0.0.0.0:{}", config.port).parse()?;
    let service = NodeService { store };

    info!("gRPC server listening on {}", addr);

    Server::builder()
        .add_service(KvStoreServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}