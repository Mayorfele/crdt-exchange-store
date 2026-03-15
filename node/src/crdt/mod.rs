use shared::types::{Entry, EntryValue, source_priority};
use crate::vector_clock::{compare, merge};

// ── The core merge function ───────────────────────────────────
// Called when gossip detects two nodes have different values
// for the same key. Decides which entry wins or combines them.
pub fn merge_entries(local: &Entry, incoming: &Entry) -> Entry {
    use crate::vector_clock::ClockOrdering::*;

    match compare(&local.clock, &incoming.clock) {
        // local is newer — keep it, ignore incoming
        After => local.clone(),

        // incoming is newer — take it
        Before | Equal => incoming.clone(),

        // genuine conflict — delegate to the specific CRDT type
        Concurrent => merge_concurrent(local, incoming),
    }
}

// ── Concurrent merge — delegates by CRDT type ────────────────
fn merge_concurrent(local: &Entry, incoming: &Entry) -> Entry {
    let merged_clock = merge(&local.clock, &incoming.clock);

    let merged_value = match (&local.value, &incoming.value) {
        // LWW-Register: tiebreak by node_id alphabetically
        (EntryValue::Register(_), EntryValue::Register(_)) => {
            if local.node_id >= incoming.node_id {
                local.value.clone()
            } else {
                incoming.value.clone()
            }
        }

        // G-Counter: max per slot
        (EntryValue::GCounter(a), EntryValue::GCounter(b)) => {
            let len = a.len().max(b.len());
            let merged = (0..len)
                .map(|i| {
                    let x = a.get(i).copied().unwrap_or(0);
                    let y = b.get(i).copied().unwrap_or(0);
                    x.max(y)
                })
                .collect();
            EntryValue::GCounter(merged)
        }

        // PN-Counter: max per slot for both increments and decrements
        (EntryValue::PnCounter(inc_a, dec_a), EntryValue::PnCounter(inc_b, dec_b)) => {
            let inc_len = inc_a.len().max(inc_b.len());
            let dec_len = dec_a.len().max(dec_b.len());

            let merged_inc = (0..inc_len)
                .map(|i| inc_a.get(i).copied().unwrap_or(0).max(inc_b.get(i).copied().unwrap_or(0)))
                .collect();

            let merged_dec = (0..dec_len)
                .map(|i| dec_a.get(i).copied().unwrap_or(0).max(dec_b.get(i).copied().unwrap_or(0)))
                .collect();

            EntryValue::PnCounter(merged_inc, merged_dec)
        }

        // OR-Set: union of both sets, preserving remove tags
        (EntryValue::OrSet(a), EntryValue::OrSet(b)) => {
            let mut merged = a.clone();
            for item in b {
                if !merged.iter().any(|x| x.tag == item.tag) {
                    merged.push(item.clone());
                }
            }
            EntryValue::OrSet(merged)
        }

        // mismatched types — local wins by default
        _ => local.value.clone(),
    }; 

    Entry {
        key: local.key.clone(),
        value: merged_value,
        crdt_type: local.crdt_type.clone(),
        clock: merged_clock,
        node_id: local.node_id.clone(),
        source: if source_priority(&local.source) >= source_priority(&incoming.source) {
        local.source.clone()
    } else {
        incoming.source.clone()
    },
    }
}