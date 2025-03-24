use memtable::{mem_table_builder::MemTableBuilder, MemTable};
use sstable::SSTable;
use std::path::PathBuf;

#[derive(Debug)]
pub struct LsmDatabase {
    pub primary: MemTable,
    pub tables: Vec<SSTable>,
    pub parent_directory: PathBuf,
}

impl LsmDatabase {
    fn new(parent_directory: String) -> Self {
        Self {
            primary: MemTableBuilder::default().max_entries(1000).build(),
            tables: Vec::new(),
            parent_directory: parent_directory.into(),
         }
    }
}
