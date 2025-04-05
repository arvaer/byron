use key_value::KeyValue;
use memtable::{mem_table_builder::MemTableBuilder, MemTable, MemTableOperations};
use sstable::{builder::SSTableFeatures, error::SSTableError, SSTable};
use std::{mem, path::PathBuf, sync::Arc, thread};

use crate::{error::LsmError, lsm_operators::LsmSearchOperators};

#[derive(Debug)]
pub struct Level {
    pub inner: Vec<Arc<SSTable>>,
    pub depth: usize,
    pub width: usize,
    pub total_entries: usize
}

#[derive(Debug)]
pub struct LsmDatabase {
    pub primary: MemTable,
    pub tables: Vec<Arc<SSTable>>,
    pub capacity_expansion_factor: f64,
    pub parent_directory: PathBuf,
    pub levels: Vec<Level>,
    pub base_fpr: f64
}

impl Default for LsmDatabase {
    fn default() -> Self {
        Self {
            primary: MemTable::default(),
            tables: Vec::new(),
            parent_directory: PathBuf::from("./data"),
            capacity_expansion_factor: 1.618,
            levels: Vec::new(),
            base_fpr: 0.005
        }
    }
}

impl LsmDatabase {
    pub fn new(parent_directory: String, capacity_expansion_factor: Option<f64>) -> Self {
        Self {
            primary: MemTableBuilder::default().max_entries(1000).build(),
            tables: Vec::new(),
            parent_directory: parent_directory.into(),
            capacity_expansion_factor: capacity_expansion_factor.unwrap_or(1.618),
            levels: Vec::new(),
            base_fpr: 0.005
        }
    }

    fn flash(&mut self) -> std::thread::JoinHandle<Arc<SSTable>> {
        let mut old_table = mem::replace(
            &mut self.primary,
            MemTableBuilder::default().max_entries(1000).build(),
        );

        let parent_directory = self.parent_directory.clone();
        let tables_len = self.tables.len();
        let features = self.calculate_sstable_features();

        thread::spawn(move || {
            let path = parent_directory.join(format!("sstable-id-{}", tables_len));
            old_table
                .flush(path, features)
                .expect("Failed to flush memtable")
        })
    }

    pub fn calculate_sstable_features(&self) -> SSTableFeatures {
        SSTableFeatures {
            item_count: 1000,
            fpr: 0.01,
        }
    }
}

impl LsmSearchOperators for LsmDatabase {
    fn get(&self, key: String) -> Result<Arc<KeyValue>, LsmError> {
        if let Some(kv) = self.primary.get(&key) {
            return Ok(kv.into());
        }

        for sstable in self.tables.iter() {
            match sstable.get(key.clone()) {
                Ok(kv) => return Ok(kv),
                Err(SSTableError::KeyNotfound) => continue,
                Err(e) => return Err(LsmError::SSTable(e)),
            }
        }
        Err(LsmError::KeyNotFound)
    }

    fn put(&mut self, key: String, value: String) {
        self.primary.put(key, value);
        if self.primary.at_capacity() {
            let sstable = self.flash().join().expect("Flushing thread panicked");
            self.tables.push(sstable);
        }
    }

    fn range() {
        todo!();
    }
}
