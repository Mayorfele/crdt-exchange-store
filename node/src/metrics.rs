// Phase 1 stubs — these get wired up properly in Phase 2
// when Prometheus is added. For now they're just counters
// in memory so the rest of the code can call them freely.
use std::sync::atomic::{AtomicU64, Ordering};

static READS: AtomicU64 = AtomicU64::new(0);
static WRITES: AtomicU64 = AtomicU64::new(0);
static GOSSIP_ROUNDS: AtomicU64 = AtomicU64::new(0);
static CONFLICTS_RESOLVED: AtomicU64 = AtomicU64::new(0);

pub fn inc_reads() {
    READS.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_writes() {
    WRITES.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_gossip_rounds() {
    GOSSIP_ROUNDS.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_conflicts_resolved() {
    CONFLICTS_RESOLVED.fetch_add(1, Ordering::Relaxed);
}

pub fn snapshot() -> (u64, u64, u64, u64) {
    (
        READS.load(Ordering::Relaxed),
        WRITES.load(Ordering::Relaxed),
        GOSSIP_ROUNDS.load(Ordering::Relaxed),
        CONFLICTS_RESOLVED.load(Ordering::Relaxed),
    )
}