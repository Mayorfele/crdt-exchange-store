use serde::{Deserialize, Serialize};

// ── Basic aliases ────────────────────────────────────────────
pub type NodeId = String;
pub type Key = String;

// ── Vector Clock ─────────────────────────────────────────────
// A vector clock is just a list of counters, one per node.
// We use a Vec<u64> because the number of nodes could vary.
pub type VectorClock = Vec<u64>;

// ── CRDT Types ───────────────────────────────────────────────
// Every value stored in the cluster carries a tag telling
// the system which CRDT type it is, so it knows which
// merge function to call when a conflict is detected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CrdtType {
    LwwRegister,  // single value, last write wins via vector clock
    GCounter,     // counter that only goes up
    PnCounter,    // counter that goes up and down
    OrSet,        // set that supports add and remove correctly
}

// ── A stored entry ───────────────────────────────────────────
// This is what actually lives in the node's HashMap.
// Not a raw value — a value plus all the metadata
// needed to merge it correctly with another node's version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub key: Key,
    pub value: EntryValue,
    pub crdt_type: CrdtType,
    pub clock: VectorClock,
    pub node_id: NodeId,  // which node last wrote this
}

// ── Entry values ─────────────────────────────────────────────
// Different CRDT types store different shapes of data.
// LwwRegister stores a plain string (your exchange rate).
// GCounter stores a list of per-node counts.
// PnCounter stores two lists: increments and decrements.
// OrSet stores a list of tagged items.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntryValue {
    Register(String),               // e.g. "0.921"
    GCounter(Vec<u64>),             // [3, 1, 2] one slot per node
    PnCounter(Vec<u64>, Vec<u64>),  // (increments, decrements)
    OrSet(Vec<OrSetItem>),          // tagged items
}

// ── OR-Set item ──────────────────────────────────────────────
// Each item in an OR-Set has a unique tag so removes
// are precise — you remove exactly the item you added,
// not every item with that value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrSetItem {
    pub value: String,
    pub tag: String,   // a unique ID generated at add time
    pub removed: bool,
}

// ── Node address ─────────────────────────────────────────────
// Used by gossip to know where peers are.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAddr {
    pub id: NodeId,
    pub host: String,
    pub port: u16,
}

impl NodeAddr {
    pub fn to_addr(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rate {
    pub pair: String,    // e.g. "USD/EUR"
    pub value: f64,      // e.g. 0.921
    pub source: String,  // e.g. "frankfurter" or "coingecko"
}

impl Rate {
    pub fn to_key(&self) -> Key {
        format!("rates:fiat:{}", self.pair)
    }

    pub fn to_value(&self) -> String {
        self.value.to_string()
    }
}