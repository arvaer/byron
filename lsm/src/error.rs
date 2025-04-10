use std::sync::mpsc::TryRecvError;

use thiserror::Error;
use tokio::task::JoinError;

#[derive(Debug, Error)]
pub enum LsmError {
    #[error("SSTable error: {0}")]
    SSTable(#[from] sstable::error::SSTableError),

    #[error("MemTable error: {0}")]
    MemTable(#[from] memtable::error::MemTableError),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Wall_E Compcation Error: {0}")]
    TokioCompactionError(#[from] JoinError),

    #[error("Other error: {0}")]
    Other(String),
}

