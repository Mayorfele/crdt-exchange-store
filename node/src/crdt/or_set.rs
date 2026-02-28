use shared::types::{EntryValue, OrSetItem};
use uuid::Uuid;

pub fn new() -> EntryValue {
    EntryValue::OrSet(vec![])
}

// Adding generates a unique tag for this specific add operation
pub fn add(value: &mut EntryValue, item: String) {
    if let EntryValue::OrSet(ref mut items) = value {
        items.push(OrSetItem {
            value: item,
            tag: Uuid::new_v4().to_string(),
            removed: false,
        });
    }
}

// Remove marks all matching items as removed — not deleted
// This is what makes OR-Set correct vs a naive set
pub fn remove(value: &mut EntryValue, item: &str) {
    if let EntryValue::OrSet(ref mut items) = value {
        for i in items.iter_mut() {
            if i.value == item {
                i.removed = true;
            }
        }
    }
}

// Read returns only items that haven't been removed
pub fn read(value: &EntryValue) -> Vec<&str> {
    if let EntryValue::OrSet(items) = value {
        items.iter()
            .filter(|i| !i.removed)
            .map(|i| i.as_str())
            .collect()
    } else {
        vec![]
    }
}