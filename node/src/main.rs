mod vector_clock;
mod crdt;
mod wal;
mod store;
mod gossip;
mod config;
mod metrics;
mod server;
mod redis_store;
mod pubsub;

use std::sync::Arc;
use tokio::sync::Mutex;
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
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("info".parse()?))
        .init();

    let config = Config::parse();

    metrics::init();
    tokio::spawn(metrics::serve_metrics(config.metrics_port));
    
    info!(
        "starting node {} on port {} with {} peers",
        config.node_id,
        config.port,
        config.peers().len()
    );

    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let store = KvStore::new(
        config.node_id.clone(),
        config.node_index,
        config.num_nodes,
        &redis_url,
    ).await?;

    let store = Arc::new(Mutex::new(store));

    let gossip_store = store.clone();
    let peers = config.peers();
    let gossip_interval = config.gossip_interval;

    tokio::spawn(async move {
        gossip::run_gossip(gossip_store, peers, gossip_interval).await;
    });

    let pubsub_store = store.clone();
    let pubsub_redis_url = redis_url.clone();

    tokio::spawn(async move {
        pubsub::run_subscriber(pubsub_store, pubsub_redis_url).await;
    });

    let addr = format!("0.0.0.0:{}", config.port).parse()?;
    let service = NodeService { store };

    info!("gRPC server listening on {}", addr);

    Server::builder()
        .add_service(KvStoreServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}