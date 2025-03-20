use memtable::MemTable;
use crate::SSTable;

pub trait SSTableOps{
    fn create(memtable:&MemTable) -> SSTable;
    fn merge

}
