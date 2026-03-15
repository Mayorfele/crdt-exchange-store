pub mod digest;
pub mod delta;

use std::time::Duration;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{info, warn};
use shared::types::{Entry, NodeAddr};
use crate::store::KvStore;
use kvstore_proto::kvstore::kv_store_client::KvStoreClient;
use kvstore_proto::kvstore::{GossipRequest, GossipEntry};

pub async fn run_gossip(
    store: Arc<Mutex<KvStore>>,
    peers: Vec<NodeAddr>,
    interval_secs: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

    loop {
        interval.tick().await;

        if peers.is_empty() {
            continue;
        }

        let peer = pick_random_peer(&peers);

        match gossip_with_peer(store.clone(), peer).await {
            Ok(_) => info!("gossip with {} succeeded", peer.id),
            Err(e) => warn!("gossip with {} failed: {}", peer.id, e),
        }
    }
}

async fn gossip_with_peer(
    store: Arc<Mutex<KvStore>>,
    peer: &NodeAddr,
) -> Result<(), Box<dyn std::error::Error>> {

    // get all local entries — await because store is async now
    let local_entries = {
        let store = store.lock().await;
        store.all_entries().await?
    };

    let digest: Vec<GossipEntry> = local_entries
        .iter()
        .map(|e| GossipEntry {
            key: e.key.clone(),
            value: String::new(),
            vector_clock: e.clock.clone(),
            crdt_type: format!("{:?}", e.crdt_type),
        })
        .collect();

    let channel = Channel::from_shared(peer.to_addr())?
        .connect()
        .await?;

    let mut client = KvStoreClient::new(channel);

    let response: tonic::Response<kvstore_proto::kvstore::GossipResponse> =
        client.gossip(GossipRequest {
            entries: digest,
        }).await?;

    let incoming_entries = response.into_inner().entries;

    if !incoming_entries.is_empty() {
        let store = store.lock().await;
        for proto_entry in incoming_entries {
            if let Some(entry) = proto_to_entry(proto_entry) {
                store.apply_gossip(entry).await?;
            }
        }
    }

    Ok(())
}

fn pick_random_peer(peers: &[NodeAddr]) -> &NodeAddr {
    let idx = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as usize) % peers.len();
    &peers[idx]
}

fn proto_to_entry(proto: GossipEntry) -> Option<Entry> {
    use shared::types::{CrdtType, EntryValue};

    let crdt_type = match proto.crdt_type.as_str() {
        "LwwRegister" => CrdtType::LwwRegister,
        "GCounter"    => CrdtType::GCounter,
        "PnCounter"   => CrdtType::PnCounter,
        "OrSet"       => CrdtType::OrSet,
        _             => return None,
    };

    Some(Entry {
        key: proto.key,
        value: EntryValue::Register(proto.value),
        crdt_type,
        clock: proto.vector_clock,
        node_id: String::new(),
        source: "gossip".to_string(),
    })
}