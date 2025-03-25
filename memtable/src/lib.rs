pub mod error;
pub mod mem_table_builder;
mod vector_mem_table;

use std::{path::PathBuf, sync::Arc};
use key_value::KeyValue;
use sstable::{builder::SSTableFeatures, SSTable};
use vector_mem_table::VectorMemTable;

pub trait MemTableOperations {
    fn put(&mut self, key: String, value: String);
    fn get(&self, key: &str) -> Option<Box<KeyValue>>;
    fn at_capacity(&self) -> bool;
    fn flush( &mut self, path: PathBuf, table_params: SSTableFeatures,) -> Result<Arc<SSTable>, crate::error::MemTableError>;
}

#[derive(Debug)]
pub enum DataStructure {
    Vector(VectorMemTable),
}

impl Default for DataStructure {
    fn default() -> Self {
        DataStructure::Vector(VectorMemTable::default())
    }
}

#[derive(Debug, Default)]
pub struct MemTable {
    inner: DataStructure,
}

impl MemTableOperations for MemTable {
    fn put(&mut self, key: String, value: String) {
        match &mut self.inner {
            //take exclusive reference to self.inner
            DataStructure::Vector(memtable) => memtable.put(key, value),
            _ => unimplemented!(),
        }
    }

    fn get(&self, key: &str) -> Option<Box<KeyValue>> {
        match &self.inner {
            //shared reference
            DataStructure::Vector(memtable) => memtable.get(key),
            _ => unimplemented!(),
        }
    }

    fn at_capacity(&self) -> bool {
        match &self.inner {
            DataStructure::Vector(memtable) => memtable.at_capacity(),
            _ => unimplemented!(),
        }
    }

    fn flush( &mut self, path: PathBuf, table_params: SSTableFeatures,) -> Result<Arc<SSTable>, crate::error::MemTableError>{
        match &mut self.inner {
            DataStructure::Vector(memtable) => memtable.flush(path, table_params),
            _ => unimplemented!(),
        }
    }
}
