use shared::types::{EntryValue, VectorClock};

// Increment this node's slot in the counter
pub fn increment(value: &mut EntryValue, node_index: usize) {
    if let EntryValue::GCounter(ref mut counts) = value {
        if node_index < counts.len() {
            counts[node_index] += 1;
        }
    }
}

// The value of a G-Counter is just the sum of all slots
pub fn read(value: &EntryValue) -> u64 {
    if let EntryValue::GCounter(counts) = value {
        counts.iter().sum()
    } else {
        0
    }
}

pub fn new(num_nodes: usize) -> EntryValue {
    EntryValue::GCounter(vec![0u64; num_nodes])
}