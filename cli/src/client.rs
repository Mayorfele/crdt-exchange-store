use kvstore_proto::kvstore::kv_store_client::KvStoreClient;
use kvstore_proto::kvstore::{GetRequest, SetRequest, DeleteRequest};

// ── Get ──────────────────────────────────────────────────────
pub async fn get(
    node: &str,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KvStoreClient::connect(node.to_string()).await?;

    let response = client.get(GetRequest {
        key: key.clone(),
    }).await?.into_inner();

    if response.found {
        println!("{} = {}", key, response.value);
    } else {
        println!("{} → not found", key);
    }

    Ok(())
}

// ── Set ──────────────────────────────────────────────────────
pub async fn set(
    node: &str,
    key: String,
    value: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KvStoreClient::connect(node.to_string()).await?;

    let response = client.set(SetRequest {
        key: key.clone(),
        value: value.clone(),
    }).await?.into_inner();

    if response.success {
        println!("✓ SET {} = {}", key, value);
    } else {
        println!("✗ SET failed");
    }

    Ok(())
}

// ── Delete ───────────────────────────────────────────────────
pub async fn delete(
    node: &str,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KvStoreClient::connect(node.to_string()).await?;

    let response = client.delete(DeleteRequest {
        key: key.clone(),
    }).await?.into_inner();

    if response.success {
        println!("✓ DELETED {}", key);
    } else {
        println!("✗ DELETE failed");
    }

    Ok(())
}