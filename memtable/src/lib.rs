mod error;
pub mod mem_table_builder;
mod vector_mem_table;

use error::MemTableError;
use sstable::SSTable;
use vector_mem_table::VectorMemTable;


pub trait MemTableOperations {
    fn put(&mut self, key: String, value: String);
    fn get(&self, key: &str) -> Option<&String>;
    fn capacity(&self) -> usize;
    fn flush(&self) -> Result<SSTable, MemTableError>;
}

#[derive(Debug)]
pub enum DataStructure {
    Vector(VectorMemTable),
}

#[derive(Debug)]
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

    fn get(&self, key: &str) -> Option<&String> {
        match &self.inner {
            //shared reference
            DataStructure::Vector(memtable) => memtable.get(key),
            _ => unimplemented!(),
        }
    }

    fn capacity(&self) -> usize {
        match &self.inner {
            DataStructure::Vector(memtable) => memtable.capacity(),
            _ => unimplemented!(),
        }
    }

    fn flush(&self) -> Result<SSTable, MemTableError> {
        match &self.inner {
            DataStructure::Vector(memtable) => memtable.flush(),
            _ => unimplemented!(),
        }
    }
}
