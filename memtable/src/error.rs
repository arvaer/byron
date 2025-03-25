use sstable::error::SSTableError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MemTableError {
    #[error("SSTable error: {0}")]
    SSTable(#[from] SSTableError),
}

