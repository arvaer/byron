use std::{fs::File, path::Path, sync::Arc};

use bloomfilter::Bloom;

mod error;
mod operations;

pub struct SSTableFeatures {}
#[derive(Debug, Default)]
pub struct SSTableConfig {}
#[derive(Debug)]
pub struct FencePointer{}

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
