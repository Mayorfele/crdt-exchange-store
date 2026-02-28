use tracing::info;
use kvstore_proto::kvstore::kv_store_client::KvStoreClient;
use kvstore_proto::kvstore::SetRequest;

// ── Write a rate into the cluster ────────────────────────────
// Connects to the target node via gRPC and calls Set.
// This is called by both frankfurter.rs and coingecko.rs
// after fetching a fresh rate from their respective APIs.
pub async fn write_rate(
    target_node: &str,
    key: String,
    value: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KvStoreClient::connect(target_node.to_string()).await?;

    let response = client.set(SetRequest {
        key: key.clone(),
        value: value.clone(),
    }).await?;

    if response.into_inner().success {
        info!("wrote {} = {} to cluster", key, value);
    }

    Ok(())
}