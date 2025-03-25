use sstable::SSTable;

use crate::lsm_database::LsmDatabase;

trait LsmCompactionOperators {
    fn compact();
    fn merge_tables(a: SSTable, b: SSTable);
}

impl LsmCompactionOperators for LsmDatabase {
    fn compact() {}

    fn merge_tables(a: SSTable, b: SSTable) {
        // both tables should already be sorted.
    }
}
