# Byron: Log-Structured Merge Tree Database

Byron is a high-performance key-value database implementation built on the Log-Structured Merge Tree (LSM) architecture, written in Rust. It provides efficient storage and retrieval mechanisms for key-value pairs with support for standard operations like put, get, range queries, and delete.

## Features

- **Efficient Write Optimization**: Uses a memory-first approach for fast writes
- **Persistent Storage**: Automatically flushes data to disk in a compact format
- **Configurable Storage Hierarchy**: Balances memory usage and performance
- **Space Efficiency**: Employs delta encoding for key compression
- **Fast Lookups**: Utilizes bloom filters and hash indices
- **Modular Architecture**: Composed of independent, reusable components

## Architecture

Byron is organized as a Rust workspace with several modular components:

### Key Components

1. **Key-Value Module**: Core data structures for key-value pairs
   - Regular and delta-encoded representations

2. **Memtable Module**: In-memory storage for recent writes
   - Vector-based implementation
   - Support for configurable capacity

3. **SSTable Module**: Persistent storage on disk
   - Block-based storage with delta encoding
   - Bloom filters for efficient key existence checking
   - Hash indices for faster lookups
   - Support for large values spanning multiple blocks

4. **LSM Module**: Database engine that coordinates all components
   - Multi-level storage hierarchy
   - Background compaction for improved read performance

5. **Server**: Lightweight grpc server
   - Uses GRPC to create a database connection
   - Asynchronous to improve speeds

## Data Flow

1. New writes go to the in-memory primary memtable
2. When the primary memtable reaches capacity, it is flushed to disk as an SSTable
3. Multiple SSTables are periodically compacted to improve read performance
4. Reads check the memtables first, then search through SSTables from newest to oldest

## Usage

```rust
use lsm::lsm_database::LsmDatabase;
use lsm::lsm_operators::LsmSearchOperators;

fn main() {
    // Create a new database with data stored in the specified directory
    let mut db = LsmDatabase::new("./data".to_string());

    // Insert key-value pairs
    db.put("key1".to_string(), "value1".to_string());
    db.put("key2".to_string(), "value2".to_string());

    // Retrieve values
    match db.get("key1".to_string()) {
        Ok(value) => println!("Found: {}", value),
        Err(_) => println!("Key not found")
    }
}
```

## Project Status

Byron is currently under active development. Core functionality is implemented, but some features remain in progress.

