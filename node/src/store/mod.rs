use std::collections::HashMap;
use std::path::PathBuf;
use dashmap::DashMap;
use shared::types::{Entry, Key, NodeId};
use crate::vector_clock;
use crate::crdt;
use crate::wal::{Wal, WalOp};
use crate::wal::snapshot::Snapshot;

// ── KvStore ──────────────────────────────────────────────────
// The central state of a node.
// DashMap is a concurrent HashMap — safe to share across
// async tasks without wrapping in a Mutex.
pub struct KvStore {
    data: DashMap<Key, Entry>,
    wal: Wal,
    node_id: NodeId,
    node_index: usize,   // this node's slot in the vector clock
    num_nodes: usize,    // total nodes in cluster (3)
    snapshot_path: PathBuf,
    write_count: std::sync::atomic::AtomicU64,
    snapshot_interval: u64, // take snapshot every N writes
}

impl KvStore {
    // ── Boot up ──────────────────────────────────────────────
    // Creates a new store, replays the WAL if one exists,
    // and loads the latest snapshot if one exists.
    pub fn new(
        node_id: NodeId,
        node_index: usize,
        num_nodes: usize,
        wal_path: PathBuf,
        snapshot_path: PathBuf,
    ) -> std::io::Result<Self> {
        let data = DashMap::new();

        // load snapshot first if it exists
        if let Some(entries) = Snapshot::load(&snapshot_path)? {
            for (key, entry) in entries {
                data.insert(key, entry);
            }
        }

        // then replay WAL on top of snapshot
        let wal_ops = Wal::replay(&wal_path)?;
        for op in wal_ops {
            match op {
                WalOp::Set(entry) => { data.insert(entry.key.clone(), entry); }
                WalOp::Delete(key) => { data.remove(&key); }
            }
        }

        let wal = Wal::open(wal_path)?;

        Ok(Self {
            data,
            wal,
            node_id,
            node_index,
            num_nodes,
            snapshot_path,
            write_count: std::sync::atomic::AtomicU64::new(0),
            snapshot_interval: 100, // snapshot every 100 writes
        })
    }

    // ── Get ──────────────────────────────────────────────────
    pub fn get(&self, key: &str) -> Option<Entry> {
        self.data.get(key).map(|e| e.clone())
    }

    // ── Set ──────────────────────────────────────────────────
    // The full write path:
    // 1. Build the entry with an incremented vector clock
    // 2. Append to WAL
    // 3. Apply to in-memory store
    // 4. Maybe take a snapshot
    pub fn set(&mut self, key: Key, value_str: String) -> std::io::Result<()> {
        // get or create the current clock for this key
        let mut clock = self.data
            .get(&key)
            .map(|e| e.clock.clone())
            .unwrap_or_else(|| vector_clock::new_clock(self.num_nodes));

        // increment this node's slot
        vector_clock::increment(&mut clock, self.node_index);

        let entry = Entry {
            key: key.clone(),
            value: shared::types::EntryValue::Register(value_str),
            crdt_type: shared::types::CrdtType::LwwRegister,
            clock,
            node_id: self.node_id.clone(),
        };

        // WAL first — disk before memory
        self.wal.append(&WalOp::Set(entry.clone()))?;

        // then memory
        self.data.insert(key, entry);

        // check if we should snapshot
        self.maybe_snapshot()?;

        Ok(())
    }

    // ── Delete ───────────────────────────────────────────────
    pub fn delete(&mut self, key: &str) -> std::io::Result<()> {
        self.wal.append(&WalOp::Delete(key.to_string()))?;
        self.data.remove(key);
        self.maybe_snapshot()?;
        Ok(())
    }

    // ── Apply gossip entry ───────────────────────────────────
    // Called by the gossip handler when a peer sends us an entry.
    // We merge it with our local version using CRDT logic.
    pub fn apply_gossip(&mut self, incoming: Entry) -> std::io::Result<()> {
        let key = incoming.key.clone();

        let merged = if let Some(local) = self.data.get(&key) {
            crdt::merge_entries(&local.clone(), &incoming)
        } else {
            // we don't have this key at all — just take incoming
            incoming
        };

        self.wal.append(&WalOp::Set(merged.clone()))?;
        self.data.insert(key, merged);

        Ok(())
    }

    // ── All entries ──────────────────────────────────────────
    // Used by gossip to get everything this node knows about
    // so it can send a digest to peers.
    pub fn all_entries(&self) -> Vec<Entry> {
        self.data.iter().map(|e| e.clone()).collect()
    }

    // ── Maybe snapshot ───────────────────────────────────────
    // Every N writes, serialize the full store to disk
    // and truncate the WAL.
    fn maybe_snapshot(&mut self) -> std::io::Result<()> {
        let count = self.write_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if count % self.snapshot_interval == 0 && count > 0 {
            let entries: HashMap<Key, Entry> = self.data
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect();

            Snapshot::save(&self.snapshot_path, &entries)?;
            self.wal.truncate()?;
        }

        Ok(())
    }
}