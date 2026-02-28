use shared::types::Entry;

// ── Digest entry ─────────────────────────────────────────────
// A lightweight summary of one entry — just the key and clock.
// No value. Used in Round 1 of gossip to let a peer know
// what keys we have and how up-to-date our clocks are.
#[derive(Debug, Clone)]
pub struct DigestEntry {
    pub key: String,
    pub clock: Vec<u64>,
}

// Build a digest from a list of entries
pub fn build_digest(entries: &[Entry]) -> Vec<DigestEntry> {
    entries.iter().map(|e| DigestEntry {
        key: e.key.clone(),
        clock: e.clock.clone(),
    }).collect()
}