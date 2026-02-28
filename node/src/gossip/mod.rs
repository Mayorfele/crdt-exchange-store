pub mod digest;
pub mod delta;

use std::time::Duration;
use tonic::transport::Channel;
use tracing::{info, warn};
use shared::types::{Entry, NodeAddr};
use crate::store::KvStore;
use kvstore_proto::kvstore::kv_store_client::KvStoreClient;
use kvstore_proto::kvstore::{GossipRequest, GossipEntry};
use std::sync::{Arc, Mutex};

// ── Gossip task ──────────────────────────────────────────────
// This runs as a background async task on every node.
// Every 2 seconds it picks a random peer, sends a digest
// of what it knows, receives what it's missing, and merges.
pub async fn run_gossip(
    store: Arc<Mutex<KvStore>>,
    peers: Vec<NodeAddr>,
    interval_secs: u64,
) {
    let mut interval = tokio::time::interval(
        Duration::from_secs(interval_secs)
    );

    loop {
        interval.tick().await;

        if peers.is_empty() {
            continue;
        }

        // pick a random peer
        let peer = pick_random_peer(&peers);

        match gossip_with_peer(store.clone(), peer).await {
            Ok(_) => info!("gossip with {} succeeded", peer.id),
            Err(e) => warn!("gossip with {} failed: {}", peer.id, e),
        }
    }
}

// ── Gossip with one peer ─────────────────────────────────────
async fn gossip_with_peer(
    store: Arc<Mutex<KvStore>>,
    peer: &NodeAddr,
) -> Result<(), Box<dyn std::error::Error>> {

    // ── Round 1: build our digest ────────────────────────────
    // Get all entries from our local store.
    // Convert them into GossipEntry protos — key + clock only.
    // We don't send values in the digest, just the clocks.
    let local_entries = {
        let store = store.lock().unwrap();
        store.all_entries()
    };

    let digest: Vec<GossipEntry> = local_entries
        .iter()
        .map(|e| GossipEntry {
            key: e.key.clone(),
            value: String::new(),       // empty in digest
            vector_clock: e.clock.clone(),
            crdt_type: format!("{:?}", e.crdt_type),
        })
        .collect();

    // ── Connect to peer ──────────────────────────────────────
    let channel = Channel::from_shared(peer.to_addr())?
        .connect()
        .await?;

    let mut client = KvStoreClient::new(channel);

    // ── Round 1: send digest, receive delta ──────────────────
    // We send our digest to the peer.
    // The peer looks at our clocks, compares with its own,
    // and sends back the full entries for keys where it's ahead.
    let response: tonic::Response<kvstore_proto::kvstore::GossipResponse> = 
    client.gossip(GossipRequest {
        entries: digest,
    }).await?;

    let incoming_entries = response.into_inner().entries;

    // ── Round 2: apply what the peer sent back ───────────────
    // For each entry the peer returned, merge it into our store.
    if !incoming_entries.is_empty() {
        let mut store = store.lock().unwrap();
        for proto_entry in incoming_entries {
            if let Some(entry) = proto_to_entry(proto_entry) {
                store.apply_gossip(entry)?;
            }
        }
    }

    Ok(())
}

// ── Pick a random peer ───────────────────────────────────────
fn pick_random_peer(peers: &[NodeAddr]) -> &NodeAddr {
    let idx = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as usize) % peers.len();
    &peers[idx]
}

// ── Convert proto GossipEntry → shared Entry ─────────────────
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
    })
}