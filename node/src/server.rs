use std::sync::Arc;
use tokio::sync::Mutex;
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
use crate::gossip::digest::DigestEntry;
use shared::types::{Entry, EntryValue};

pub struct NodeService {
    pub store: Arc<Mutex<KvStore>>,
}

#[tonic::async_trait]
impl KvStoreService for NodeService {

    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let key = request.into_inner().key;
        info!("GET {}", key);
        metrics::inc_reads();

        let store = self.store.lock().await;
        match store.get(&key).await {
            Some(entry) => {
                let value = match &entry.value {
                    EntryValue::Register(v) => v.clone(),
                    _ => String::new(),
                };
                Ok(Response::new(GetResponse { value, found: true }))
            }
            None => Ok(Response::new(GetResponse {
                value: String::new(),
                found: false,
            }))
        }
    }

    async fn set(
        &self,
        request: Request<SetRequest>,
    ) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        info!("SET {} = {}", req.key, req.value);
        metrics::inc_writes();

        let store = self.store.lock().await;
        match store.set(req.key, req.value, "manual".to_string()).await {
            Ok(_) => Ok(Response::new(SetResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let key = request.into_inner().key;
        info!("DELETE {}", key);
        metrics::inc_writes();

        let store = self.store.lock().await;
        match store.delete(&key).await {
            Ok(_) => Ok(Response::new(DeleteResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn gossip(
        &self,
        request: Request<GossipRequest>,
    ) -> Result<Response<GossipResponse>, Status> {
        metrics::inc_gossip_rounds();

        let incoming_digest_protos = request.into_inner().entries;

        let incoming_digest: Vec<DigestEntry> =
            incoming_digest_protos.iter().map(|e| DigestEntry {
                key: e.key.clone(),
                clock: e.vector_clock.clone(),
            }).collect();

        let store = self.store.lock().await;
        let local_entries = store.all_entries().await
            .unwrap_or_default();

        let delta = compute_delta(&local_entries, &incoming_digest);

        let response_entries: Vec<GossipEntry> = delta.iter()
            .map(|e| entry_to_proto(e))
            .collect();

        Ok(Response::new(GossipResponse {
            entries: response_entries,
        }))
    }
}

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