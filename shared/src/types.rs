use serde::{Deserialize, Serialize};

pub type NodeId = String;
pub type Key = String;


pub type VectorClock = Vec<u64>;


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CrdtType {
    LwwRegister,  // single value, last write wins via vector clock
    GCounter,     // counter that only goes up
    PnCounter,    // counter that goes up and down
    OrSet,        // set that supports add and remove correctly
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub key: Key,
    pub value: EntryValue,
    pub crdt_type: CrdtType,
    pub clock: VectorClock,
    pub node_id: NodeId,  // which node last wrote this
    pub source: String,  // ← add this line

}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntryValue {
    Register(String),               // e.g. "0.921"
    GCounter(Vec<u64>),             // [3, 1, 2] one slot per node
    PnCounter(Vec<u64>, Vec<u64>),  // (increments, decrements)
    OrSet(Vec<OrSetItem>),          // tagged items
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrSetItem {
    pub value: String,
    pub tag: String,   // a unique ID generated at add time
    pub removed: bool,
}


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

pub fn source_priority(source: &str) -> u8 {
    match source {
        "frankfurter" => 3,
        "coincap"     => 2,
        "manual"      => 1,
        _             => 0,
    }
}
