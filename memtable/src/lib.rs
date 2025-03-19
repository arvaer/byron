use vector_mem_table::VectorMemTable;

mod vector_mem_table;
pub mod mem_table_builder;

pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub trait MemTableOperations {
    fn put(&mut self, key: String, value: String);
    fn get(&self, key: &str) -> Option<&String>;
    fn capacity(&self) -> usize;
}


pub enum DataStructure {
    Vector(VectorMemTable),
}

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
}
