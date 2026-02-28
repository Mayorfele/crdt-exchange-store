pub mod snapshot;

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use shared::types::Entry;

// ── WAL Entry ────────────────────────────────────────────────
#[derive(Debug, Serialize, Deserialize)]
pub enum WalOp {
    Set(Entry),
    Delete(String),
}

// ── WAL struct ───────────────────────────────────────────────
pub struct Wal {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl Wal {
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            path,
            writer: BufWriter::new(file),
        })
    }

    pub fn append(&mut self, op: &WalOp) -> std::io::Result<()> {
        let bytes = bincode::serialize(op)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let len = bytes.len() as u32;
        self.writer.write_all(&len.to_be_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;

        Ok(())
    }

    pub fn replay(path: &PathBuf) -> std::io::Result<Vec<WalOp>> {
        if !path.exists() {
            return Ok(vec![]);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut ops = Vec::new();

        loop {
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let len = u32::from_be_bytes(len_bytes) as usize;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;

            let op: WalOp = bincode::deserialize(&buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            ops.push(op);
        }

        Ok(ops)
    }

    pub fn truncate(&self) -> std::io::Result<()> {
        File::create(&self.path)?;
        Ok(())
    }
}