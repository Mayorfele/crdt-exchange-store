use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
use tracing::info;
use crate::store::KvStore;
use crate::metrics;
use kvstore_proto::kvstore::{
    kv_store_server::KvStore as KvStoreService,
    GetRequest, GetResponse,
    SetRequest, SetResponse,
    DeleteRequest, DeleteResponse,
    GossipRequest, GossipResponse,
    GossipEntry,
};
use crate::gossip::delta::compute_delta;
use shared::types::{Entry, EntryValue};

pub struct NodeService {
    pub store: Arc<Mutex<KvStore>>,
}

#[tonic::async_trait]
impl KvStoreService for NodeService {

    // ── Get ──────────────────────────────────────────────────
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let key = request.into_inner().key;
        info!("GET {}", key);
        metrics::inc_reads();

        let store = self.store.lock().unwrap();
        match store.get(&key) {
            Some(entry) => {
                let value = match &entry.value {
                    EntryValue::Register(v) => v.clone(),
                    _ => String::new(),
                };
                Ok(Response::new(GetResponse {
                    value,
                    found: true,
                }))
            }
            None => Ok(Response::new(GetResponse {
                value: String::new(),
                found: false,
            }))
        }
    }

    // ── Set ──────────────────────────────────────────────────
    async fn set(
        &self,
        request: Request<SetRequest>,
    ) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        info!("SET {} = {}", req.key, req.value);
        metrics::inc_writes();

        let mut store = self.store.lock().unwrap();
        match store.set(req.key, req.value) {
            Ok(_) => Ok(Response::new(SetResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    // ── Delete ───────────────────────────────────────────────
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let key = request.into_inner().key;
        info!("DELETE {}", key);
        metrics::inc_writes();

        let mut store = self.store.lock().unwrap();
        match store.delete(&key) {
            Ok(_) => Ok(Response::new(DeleteResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    // ── Gossip ───────────────────────────────────────────────
    // This is the server side of gossip.
    // A peer sends us their digest.
    // We compare it against our local state.
    // We send back only the entries where we're ahead.
    async fn gossip(
        &self,
        request: Request<GossipRequest>,
    ) -> Result<Response<GossipResponse>, Status> {
        metrics::inc_gossip_rounds();

        let incoming_digest_protos = request.into_inner().entries;

        // convert proto digest entries into DigestEntry structs
        let incoming_digest: Vec<crate::gossip::digest::DigestEntry> =
            incoming_digest_protos.iter().map(|e| {
                crate::gossip::digest::DigestEntry {
                    key: e.key.clone(),
                    clock: e.vector_clock.clone(),
                }
            }).collect();

        let store = self.store.lock().unwrap();
        let local_entries = store.all_entries();

        // compute what we have that the sender is missing
        let delta = compute_delta(&local_entries, &incoming_digest);

        // convert delta entries back to proto format
        let response_entries: Vec<GossipEntry> = delta.iter()
            .map(|e| entry_to_proto(e))
            .collect();

        Ok(Response::new(GossipResponse {
            entries: response_entries,
        }))
    }
}

// ── Convert Entry → proto GossipEntry ────────────────────────
fn entry_to_proto(entry: &Entry) -> GossipEntry {
    let value = match &entry.value {
        EntryValue::Register(v) => v.clone(),
        _ => String::new(),
    };

    GossipEntry {
        key: entry.key.clone(),
        value,
        vector_clock: entry.clock.clone(),
        crdt_type: format!("{:?}", entry.crdt_type),
    }
}