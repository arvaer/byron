use crate::{MemTable, DataStructure};
use crate::VectorMemTable;

pub enum MemTableType {
    Vector,
    SkipList,
    ConcurrentHashmap,
}

pub struct MemTableBuilder {
    memtable_type: MemTableType,
    max_entries: usize,
}

impl Default for MemTableBuilder {
    fn default() -> Self {
        Self {
            memtable_type: MemTableType::Vector,
            max_entries: 10,
        }
    }
}
impl MemTableBuilder {
    pub fn memtable_type(mut self, memtable_type: MemTableType) -> Self {
        self.memtable_type = memtable_type;
        self
    }

    pub fn max_entries(mut self, max_entries: usize) -> Self {
        self.max_entries = max_entries;
        self
    }

    pub fn build(self) -> MemTable {
        let inner = match self.memtable_type {
            MemTableType::Vector => DataStructure::Vector(VectorMemTable::new(self.max_entries)),
            MemTableType::SkipList => unimplemented!("skiplist not implemented yet"),
            MemTableType::ConcurrentHashmap => {
                unimplemented!("Concurrent Hashmap not impemented yet")
            }
        };
        MemTable { inner }
    }
}
