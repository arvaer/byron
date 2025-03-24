use std::path::PathBuf;
use memtable::MemTable;
use sstable::SSTable;

pub struct LsmDatabase{
   pub  primary:MemTable,
    pub secondary: MemTable,
    pub tables: Vec<SSTable>,
    pub parent_directoy: PathBuf,
}
