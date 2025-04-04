use std::path::PathBuf;

use sstable::{builder::SSTableBuilder, streamed_builder::StreamedSSTableBuilder, SSTable};

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
        let features = self.calculate_sstable_features();
        let new_table  = StreamedSSTableBuilder::new(&file_name);

        let mut keep_going = true;
        let a_iter = a.iter().next();
        let b_iter = b.iter().next();
        while keep_going {

            keep_going = false;
        }

        Ok(())

    }
}
