use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc};

use bloomfilter::Bloom;
use key_value::{key_value_pair::DeltaEncodedKV, KeyValue};

mod builder;
mod error;
mod operations;

pub struct SSTableFeatures {
    bf_fpr: isize,
    lz: bool,
}

#[derive(Debug)]
pub struct SSTable {
    file_path: PathBuf,
    fd: Option<File>,
    bloom_filter: Bloom<String>,
    entry_count: usize,
    //    size_in_kb: usize,
    page_hash_indices: Vec<HashMap<Vec<u8>, usize>>, // One hash index per block
    fence_pointers: Vec<(Vec<u8>, usize)>,
}

impl SSTable {
    pub fn get(&self, key: String) -> Option<Vec<u8>> {
        if !self.bloom_filter.check(&key) {
            return None;
        }

        todo!()
    }

    fn find_block_with_fence_pointers(&self, key: &[u8]) -> Option<usize> {
        todo!()
    }

    fn read_block_from_disk(&self, block_idx: usize) -> Option<Vec<u8>> {
        todo!()
    }

    fn binary_search_with_restarts(
        &self,
        block_data: &[u8],
        key: &[u8],
        restart_points: &[usize],
    ) -> Option<usize> {
        todo!()
    }

    fn extract_value_at_position(&self, block_data: &[u8], position: usize) -> Option<Vec<u8>> {
        todo!()
    }
}
