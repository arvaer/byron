use thiserror::Error;

#[derive(Debug, Error)]
pub enum LsmError {
    #[error("SSTable error: {0}")]
    SSTable(#[from] sstable::error::SSTableError),

    #[error("MemTable error: {0}")]
    MemTable(#[from] memtable::error::MemTableError),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Other error: {0}")]
    Other(String),
}

