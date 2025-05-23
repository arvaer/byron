pub mod error;
pub mod mem_table_builder;
mod skiplist;
mod vector_mem_table;

use key_value::KeyValue;
use sstable::{builder::SSTableFeatures, SSTable};
use std::{path::PathBuf, sync::Arc};
use vector_mem_table::VectorMemTable;
use skiplist::CrossBeam;

#[derive(PartialEq, Debug)]
pub enum RangeResult {
    KeyNotFound,
    FirstKeyFound,
    FullSetFound,
}

pub trait MemTableOperations {
    fn put(&mut self, key: String, value: String);
    fn insert(&self, key: String, value: String);
    fn get(&self, key: &str) -> Option<Box<KeyValue>>;
    fn range(&self, from_m: &str, to_n: &str) -> (Vec<Box<KeyValue>>, RangeResult);
    fn at_capacity(&self) -> bool;
    fn current_length(&self) -> usize;
    fn max_entries(&self) -> usize;
    fn flush(
        &self,
        path: PathBuf,
        table_params: SSTableFeatures,
    ) -> Result<Arc<SSTable>, crate::error::MemTableError>;
}

#[derive(Debug)]
pub enum DataStructure {
    Vector(VectorMemTable),
    SkipList(CrossBeam)

}

impl Default for DataStructure {
    fn default() -> Self {
        DataStructure::Vector(VectorMemTable::default())
    }
}

#[derive(Debug, Default)]
pub struct MemTable {
    pub inner: DataStructure,
}

impl MemTableOperations for MemTable {
    fn put(&mut self, key: String, value: String) {
        match &mut self.inner {
            //take exclusive reference to self.inner
            DataStructure::Vector(memtable) => memtable.put(key, value),
            DataStructure::SkipList(memtable) => memtable.put(key, value),
        }
    }
    fn insert(&self, key: String, value: String) {
        match &self.inner {
            //take exclusive reference to self.inner
            DataStructure::Vector(memtable) => memtable.insert(key, value),
            DataStructure::SkipList(memtable) => memtable.insert(key, value),
        }
    }
    fn get(&self, key: &str) -> Option<Box<KeyValue>> {
        match &self.inner {
            //shared reference
            DataStructure::Vector(memtable) => memtable.get(key),
            DataStructure::SkipList(memtable) => memtable.get(key),
        }
    }

    fn range(&self, from_m: &str, to_n: &str) -> (Vec<Box<KeyValue>>, RangeResult) {
        match &self.inner {
            //shared reference
            DataStructure::Vector(memtable) => memtable.range(from_m, to_n),
            DataStructure::SkipList(memtable) => memtable.range(from_m, to_n),
        }
    }

    fn at_capacity(&self) -> bool {
        match &self.inner {
            DataStructure::Vector(memtable) => memtable.at_capacity(),
            DataStructure::SkipList(memtable) => memtable.at_capacity(),
        }
    }

    fn current_length(&self) -> usize {
        match &self.inner {
            DataStructure::Vector(memtable) => memtable.current_length(),
            DataStructure::SkipList(memtable) => memtable.current_length(),
        }
    }

    fn max_entries(&self) -> usize {
        match &self.inner {
            DataStructure::Vector(memtable) => memtable.max_entries(),
            DataStructure::SkipList(memtable) => memtable.max_entries(),
        }
    }

    fn flush(
        &self,
        path: PathBuf,
        table_params: SSTableFeatures,
    ) -> Result<Arc<SSTable>, crate::error::MemTableError> {
        match &self.inner {
            DataStructure::Vector(memtable) => memtable.flush(path, table_params),
            DataStructure::SkipList(memtable) => memtable.flush(path, table_params),
        }
    }
}
