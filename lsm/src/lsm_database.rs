use key_value::KeyValue;
use memtable::{mem_table_builder::MemTableBuilder, MemTable, MemTableOperations};
use sstable::{builder::SSTableFeatures, error::SSTableError, SSTable};
use std::{mem, path::PathBuf, sync::Arc, thread};

use crate::{error::LsmError, lsm_operators::LsmSearchOperators};

#[derive(Debug)]
pub struct LsmDatabase {
    pub primary: MemTable,
    pub tables: Vec<Arc<SSTable>>,
    pub parent_directory: PathBuf,
}

impl LsmDatabase {
    pub fn new(parent_directory: String) -> Self {
        Self {
            primary: MemTableBuilder::default().max_entries(1000).build(),
            tables: Vec::new(),
            parent_directory: parent_directory.into(),
        }
    }

    fn flash(&mut self) -> std::thread::JoinHandle<Arc<SSTable>> {
        let old_table = mem::replace(
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

    fn calculate_sstable_features(&self) -> SSTableFeatures {
        SSTableFeatures {
            lz: false,
            fpr: 0.01,
        }
    }
}

impl LsmSearchOperators for LsmDatabase {
     fn get(&self, key: String) -> Result<Arc<KeyValue>, LsmError> {
        if let Some(kv) = self.primary.get(&key) {
            return Ok(kv.into());
        }

        for sstable in self.tables.iter().rev() {
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
