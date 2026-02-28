use shared::types::Entry;
use crate::vector_clock::{compare, ClockOrdering};
use super::digest::DigestEntry;

// ── Compute delta ────────────────────────────────────────────
// Called on the receiving end of a gossip digest.
// We look at what the sender has (their digest) and compare
// it against what we have locally.
// We return only the entries where WE are ahead —
// those are the ones the sender is missing.
pub fn compute_delta(
    local_entries: &[Entry],
    incoming_digest: &[DigestEntry],
) -> Vec<Entry> {
    let mut delta = Vec::new();

    for local in local_entries {
        // find the matching digest entry from the sender
        let sender_clock = incoming_digest
            .iter()
            .find(|d| d.key == local.key)
            .map(|d| d.clock.clone())
            .unwrap_or_else(|| vec![0u64; local.clock.len()]);

        // if we're ahead or concurrent — include in delta
        match compare(&local.clock, &sender_clock) {
            ClockOrdering::After | ClockOrdering::Concurrent => {
                delta.push(local.clone());
            }
            // sender is ahead or equal — they don't need our version
            _ => {}
        }
    }

    delta
}