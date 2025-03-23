use thiserror::Error;

#[derive(Error, Debug)]
pub enum SSTableError {
    #[error("IO error: {0}")]
    FileSystemError(#[from] std::io::Error),

    #[error("Block decode error: {0}")]
    StringUTF8(#[from] std::string::FromUtf8Error),

    #[error("Invalid block size: {0}")]
    InvalidBlockSize(usize),

    #[error("Invalid item count: count must be greater than 0")]
    InvalidItemCount,

    #[error("Invalid false positive rate: {0}. Must be between 0 and 1")]
    InvalidFalsePositiveRate(f64),

    #[error("Bloom filter error: {0}")]
    BloomFilterError(String),

    #[error("Empty key not allowed")]
    EmptyKey,

    #[error("Key Not Found")]
    KeyNotfound,

    #[error("Failed to encode key-value pair")]
    EncodingError,

    #[error("Failed to build SSTable: {0}")]
    BuildError(String),

    #[error("Failed to build SSTable: {0}")]
    NoTableFound(String),
}
