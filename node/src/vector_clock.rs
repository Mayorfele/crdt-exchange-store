use shared::types::VectorClock;

// ── Ordering result ──────────────────────────────────────────
// When we compare two vector clocks we get one of four outcomes.
// This enum represents that result clearly.
#[derive(Debug, PartialEq)]
pub enum ClockOrdering {
    Before,      // local is strictly older than incoming
    After,       // local is strictly newer than incoming
    Equal,       // both clocks are identical
    Concurrent,  // neither dominates — genuine conflict
}

// ── Create a new clock ───────────────────────────────────────
// When a node starts up it needs a fresh vector clock.
// num_nodes is how many nodes are in the cluster (3 for us).
// All counters start at zero.
pub fn new_clock(num_nodes: usize) -> VectorClock {
    vec![0u64; num_nodes]
}

// ── Increment ────────────────────────────────────────────────
// Called every time THIS node writes something.
// node_index is this node's position in the clock array.
// Node A = 0, Node B = 1, Node C = 2.
pub fn increment(clock: &mut VectorClock, node_index: usize) {
    if node_index < clock.len() {
        clock[node_index] += 1;
    }
}

// ── Compare ──────────────────────────────────────────────────
// The heart of the vector clock logic.
// Compares two clocks and returns their relationship.
//
// How it works:
// If A is >= B in every slot → A is After (or Equal)
// If B is >= A in every slot → A is Before (or Equal)
// If A is greater in some slots but B in others → Concurrent
pub fn compare(local: &VectorClock, incoming: &VectorClock) -> ClockOrdering {
    // make sure both clocks are the same length
    // (they always should be in a fixed 3-node cluster)
    let len = local.len().max(incoming.len());

    let local_greater = (0..len).any(|i| {
        let l = local.get(i).copied().unwrap_or(0);
        let r = incoming.get(i).copied().unwrap_or(0);
        l > r
    });

    let incoming_greater = (0..len).any(|i| {
        let l = local.get(i).copied().unwrap_or(0);
        let r = incoming.get(i).copied().unwrap_or(0);
        r > l
    });

    match (local_greater, incoming_greater) {
        (false, false) => ClockOrdering::Equal,
        (true, false)  => ClockOrdering::After,
        (false, true)  => ClockOrdering::Before,
        (true, true)   => ClockOrdering::Concurrent,
    }
}

// ── Merge ────────────────────────────────────────────────────
// After resolving a conflict, we need a merged clock that
// represents "I now know about both of these writes."
// The merge is simple: take the max of each slot.
//
// Example:
// local    = [3, 1, 0]
// incoming = [2, 2, 1]
// merged   = [3, 2, 1]  ← max of each position
pub fn merge(local: &VectorClock, incoming: &VectorClock) -> VectorClock {
    let len = local.len().max(incoming.len());
    (0..len)
        .map(|i| {
            let l = local.get(i).copied().unwrap_or(0);
            let r = incoming.get(i).copied().unwrap_or(0);
            l.max(r)
        })
        .collect()
}

// ── Tests ────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment() {
        let mut clock = new_clock(3);
        increment(&mut clock, 0);
        assert_eq!(clock, vec![1, 0, 0]);
        increment(&mut clock, 0);
        assert_eq!(clock, vec![2, 0, 0]);
        increment(&mut clock, 1);
        assert_eq!(clock, vec![2, 1, 0]);
    }

    #[test]
    fn test_equal() {
        let a = vec![1, 1, 0];
        let b = vec![1, 1, 0];
        assert_eq!(compare(&a, &b), ClockOrdering::Equal);
    }

    #[test]
    fn test_after() {
        // local has seen everything incoming has, plus more
        let local    = vec![2, 1, 0];
        let incoming = vec![1, 1, 0];
        assert_eq!(compare(&local, &incoming), ClockOrdering::After);
    }

    #[test]
    fn test_before() {
        // incoming is strictly newer than local
        let local    = vec![1, 1, 0];
        let incoming = vec![2, 1, 0];
        assert_eq!(compare(&local, &incoming), ClockOrdering::Before);
    }

    #[test]
    fn test_concurrent() {
        // neither dominates — genuine conflict
        let local    = vec![1, 0, 0];
        let incoming = vec![0, 1, 0];
        assert_eq!(compare(&local, &incoming), ClockOrdering::Concurrent);
    }

    #[test]
    fn test_merge() {
        let local    = vec![3, 1, 0];
        let incoming = vec![2, 2, 1];
        let merged   = merge(&local, &incoming);
        assert_eq!(merged, vec![3, 2, 1]);
    }
}