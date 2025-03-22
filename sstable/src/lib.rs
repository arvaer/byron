use std::{fs::File, sync::Arc};

use bloomfilter::Bloom;
use key_value::FencePointer;

mod error;
mod operations;

pub struct SSTableFeatures {
    bf_fpr: isize,
    lz: bool,
}
#[derive(Debug, Default)]
pub struct SSTableConfig {
    block_size : usize,
    restart_interval: usize, // need to store restart pointers at the end of sstable, and then
                             // bsearch the first key in each restart


}


#[derive(Debug, Default)]
pub struct SSTable{
    file_path: String,
    fd: Option<File>,
    bloom_filter: Option<Bloom<str>>,
    fence_pointers: Option<Vec<FencePointer>>,
    config: SSTableConfig,
    entry_count: usize,
    size_in_kb: usize
}

impl SSTable {
    fn get(&self, key: Arc<str>) -> Option<Arc<str>> {
        todo!();
    }


}
