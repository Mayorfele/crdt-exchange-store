use std::sync::Arc;
use tokio::sync::Mutex;
use futures_util::StreamExt;
use tracing::{info, warn};
use shared::types::Entry;
use crate::store::KvStore;

pub async fn run_subscriber(
    store: Arc<Mutex<KvStore>>,
    redis_url: String,
) {
    loop {
        match connect_and_listen(store.clone(), &redis_url).await {
            Ok(_) => {
                info!("pubsub connection closed, reconnecting...");
            }
            Err(e) => {
                let msg = e.to_string();
                warn!("pubsub error: {}, retrying in 2s...", msg);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }
    }
}

async fn connect_and_listen(
    store: Arc<Mutex<KvStore>>,
    redis_url: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = redis::Client::open(redis_url)?;

    // use get_async_pubsub() — dedicated pubsub connection
    let mut pubsub = client.get_async_pubsub().await?;

    pubsub.subscribe("rate-updates").await?;
    info!("subscribed to rate-updates channel");

    let mut stream = pubsub.on_message();

    loop {
        match stream.next().await {
            Some(msg) => {
                let bytes: Vec<u8> = match msg.get_payload() {
                    Ok(b) => b,
                    Err(e) => {
                        warn!("failed to get payload: {}", e);
                        continue;
                    }
                };

                let entry = match bincode::deserialize::<Entry>(&bytes) {
                    Ok(e) => e,
                    Err(e) => {
                        warn!("failed to deserialize: {}", e);
                        continue;
                    }
                };

                info!("received rate update: {}", entry.key);

                {
                    let store = store.lock().await;
                    if let Err(e) = store.apply_gossip(entry).await {
                        warn!("failed to apply update: {}", e);
                    }
                }
            }
            None => break,
        }
    }

    Ok(())
}