use std::path::PathBuf;

use sstable::{builder::SSTableBuilder, SSTable};

use crate::{error::LsmError, lsm_database::LsmDatabase};

trait LsmCompactionOperators {
    fn compact();
    fn merge_tables(&mut self, a: SSTable, b: SSTable, file_name: PathBuf) -> Result<(), LsmError>;
}

impl LsmCompactionOperators for LsmDatabase {
    fn compact() {}

    fn merge_tables(&mut self, a: SSTable, b: SSTable, file_name: PathBuf) -> Result<(), LsmError> {
        let a_name = stringify!(a.file_path);
        let b_name = stringify!(b.file_path);
        let total_length : usize = self.table_sizes.get(a_name).unwrap() + self.table_sizes.get(b_name).unwrap();
        let features = self.calculate_sstable_features();
        let new_table  = SSTableBuilder::new(features,&file_name,  total_length);

        let mut keep_going = true;
        let a_iter = a.iter().next();
        let b_iter = b.iter().next();
        while keep_going {

            keep_going = false;
        }

        Ok(())

    }
}
