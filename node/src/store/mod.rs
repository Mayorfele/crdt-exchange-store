use std::path::PathBuf;
use std::collections::HashMap;
use shared::types::{Entry, Key, NodeId};
use crate::vector_clock;
use crate::crdt;
use crate::redis_store::RedisStore;

pub struct KvStore {
    redis: RedisStore,
    node_id: NodeId,
    node_index: usize,
    num_nodes: usize,
}

impl KvStore {
    pub async fn new(
        node_id: NodeId,
        node_index: usize,
        num_nodes: usize,
        redis_url: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let redis = RedisStore::new(redis_url)?;
        Ok(Self {
            redis,
            node_id,
            node_index,
            num_nodes,
        })
    }

    // ── Get ──────────────────────────────────────────────────
    pub async fn get(&self, key: &str) -> Option<Entry> {
        self.redis.get(key).await.unwrap_or(None)
    }

    // ── Set ──────────────────────────────────────────────────
    pub async fn set(
        &self,
        key: Key,
        value_str: String,
        source: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // get current clock for this key or create fresh one
        let mut clock = self.redis
            .get(&key)
            .await?
            .map(|e| e.clock)
            .unwrap_or_else(|| vector_clock::new_clock(self.num_nodes));

        // increment this node's slot
        vector_clock::increment(&mut clock, self.node_index);

        let entry = Entry {
            key: key.clone(),
            value: shared::types::EntryValue::Register(value_str),
            crdt_type: shared::types::CrdtType::LwwRegister,
            clock,
            node_id: self.node_id.clone(),
            source,
        };

        // write atomically to Redis
        self.redis.set_atomic(&entry).await?;

        // publish to other nodes via Pub/Sub
        self.redis.publish(&entry).await?;

        Ok(())
    }

    // ── Delete ───────────────────────────────────────────────
    pub async fn delete(
        &self,
        key: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.redis.delete(key).await?;
        Ok(())
    }

    // ── Apply gossip ─────────────────────────────────────────
    // Called by pubsub when another node publishes an update.
    // Runs CRDT merge and writes result back to Redis.
    pub async fn apply_gossip(
        &self,
        incoming: Entry,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = incoming.key.clone();

        let merged = if let Some(local) = self.redis.get(&key).await? {
            crdt::merge_entries(&local, &incoming)
        } else {
            incoming
        };

        self.redis.set_atomic(&merged).await?;
        Ok(())
    }

    // ── All entries ───────────────────────────────────────────
    pub async fn all_entries(
        &self,
    ) -> Result<Vec<Entry>, Box<dyn std::error::Error>> {
        self.redis.all_entries().await
    }
}