use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use shared::types::{Entry, Key};

// ── Snapshot ─────────────────────────────────────────────────
// A snapshot is a full serialized copy of the store's HashMap
// written to disk at a point in time. On restart, the node
// loads the snapshot first, then replays only the WAL entries
// that came after it — much faster than replaying the full WAL.
#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    pub entries: HashMap<Key, Entry>,
}

impl Snapshot {
    // ── Save ─────────────────────────────────────────────────
    // Serialize the entire store to disk.
    pub fn save(path: &PathBuf, entries: &HashMap<Key, Entry>) -> std::io::Result<()> {
        let snapshot = Snapshot {
            entries: entries.clone(),
        };

        let bytes = bincode::serialize(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)  // overwrite the previous snapshot
            .open(path)?;

        let mut writer = BufWriter::new(file);
        writer.write_all(&bytes)?;
        writer.flush()?;

        Ok(())
    }

    // ── Load ─────────────────────────────────────────────────
    // Read and deserialize a snapshot from disk.
    // Returns None if no snapshot file exists yet.
    pub fn load(path: &PathBuf) -> std::io::Result<Option<HashMap<Key, Entry>>> {
        if !path.exists() {
            return Ok(None);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;

        let snapshot: Snapshot = bincode::deserialize(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(Some(snapshot.entries))
    }
}