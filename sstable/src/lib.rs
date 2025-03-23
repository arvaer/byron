use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};

use bloomfilter::Bloom;
use error::SSTableError;
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
    page_hash_indices: Vec<HashMap<String, usize>>, // One hash index per block
    fence_pointers: Vec<(Arc<str>, usize)>,
}

impl SSTable {
    pub fn get(&self, key: String) -> Result<Arc<str>, SSTableError> {
        if !self.bloom_filter.check(&key) {
            return Err(SSTableError::KeyNotfound);
        }

        let block_idx = self.find_block_with_fence_pointers(key).unwrap_or((0, 1));
        let block_data = self.read_block_from_disk(block_idx)?;
        let restart_points = &self.restart_indices[block_idx];
        let entry_position = self.binary_search_with_restarts(block_data, key, restart_points)?;
        if let Some(position) = self.page_hash_indices[block_idx.0].get(key.as_bytes()) {
            return self.extract_value_at_position(block_data, position);
        }

        todo!()
    }

    /*

     self.extract_value_at_position(block_data, entry_position)
    */

    // this is not working correctly for case sensetivity. need to fix this in the mean time using
    // linear search because it works
    fn find_block_with_fence_pointers(&self, key: String) -> Option<(usize, usize)> {
        // say we get a key with first letter b, and fp 1 is a, fp 2 is B
        // in that case we want to return 1
        // so we binary search over the key and search the range inbetween fence_pounts[mid], fence_pointers[mid+1]
        if self.fence_pointers.is_empty() {
            return None;
        }

        if key.as_str() < self.fence_pointers[0].0.as_ref() {
            return Some((0, 1));
        }

        if key.as_str() >= self.fence_pointers.last().unwrap().0.as_ref() {
            return Some((self.fence_pointers.len() - 1, self.fence_pointers.len()));
        }

        // for keys between fence pointers, bs
        let mut left = 0;
        let mut right = self.fence_pointers.len() - 1;

        while left < right {
            let mid = left + (right - left) / 2;
            let next = mid + 1;
            let current_key = self.fence_pointers[mid].0.as_ref();
            let next_key = self.fence_pointers[next].0.as_ref();

            if current_key == key.as_str() {
                return Some((mid, next));
            }
            if current_key == next_key && key.as_str() == current_key {
                return Some((mid, next));
            }

            if mid + 1 < self.fence_pointers.len()
                && key.as_str() >= self.fence_pointers[mid].0.as_ref()
                && key.as_str() < self.fence_pointers[mid + 1].0.as_ref()
            {
                return Some((mid, next));
            }

            if key.as_str() < self.fence_pointers[mid].0.as_ref() {
                right = mid;
            } else {
                left = mid + 1;
            }
        }

        Some((left - 1, left))
    }

    fn read_block_from_disk(&self, offset: (usize, usize)) -> Result<Arc<[u8]>, SSTableError> {
        let mut reader: BufReader<&File>;
        let file: File;
        if let Some(file) = &self.fd {
            reader = BufReader::new(file);
        } else {
            file = File::open(&self.file_path).map_err(SSTableError::FileSystemError)?;
            reader = BufReader::new(&file);
        }

        let ffw = b"SSTB".len() + offset.0;

        reader
            .seek(SeekFrom::Start(ffw as u64))
            .map_err(SSTableError::FileSystemError)?;
        let mut block_data = vec![0u8, offset.1 as u8 - offset.0 as u8];
        let _ = reader.read_exact(&mut block_data);
        Ok(Arc::from(block_data.into_boxed_slice()))
    }

    fn binary_search_with_restarts(
        &self,
        block_data: &[u8],
        key: &[u8],
        restart_points: &[usize],
    ) -> Option<usize> {
        todo!()
    }

    fn extract_value_at_position(&self, block_data: Arc<[u8]>, position: usize) -> Option<Arc<u8>> {
        Some(block_data[position..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bloomfilter::Bloom;
    use std::path::PathBuf;
    use std::sync::Arc;

    // Helper constructor for testing purposes
    impl SSTable {
        fn new_for_tests(fence_pointers: Vec<(Arc<str>, usize)>) -> Self {
            // Create a minimal valid bloom filter for testing
            let bloom = Bloom::new_for_fp_rate(100, 0.01).unwrap();

            Self {
                file_path: PathBuf::from("test.sst"),
                fd: None,
                bloom_filter: bloom,
                entry_count: fence_pointers.len(),
                page_hash_indices: Vec::new(),
                fence_pointers,
            }
        }
    }

    // Helper function to create fence pointers
    fn create_fence_pointers(keys: Vec<&str>) -> Vec<(Arc<str>, usize)> {
        keys.into_iter()
            .enumerate()
            .map(|(i, k)| (Arc::from(k), i))
            .collect()
    }

    #[test]
    fn test_empty_fence_pointers() {
        let sstable = SSTable::new_for_tests(vec![]);
        let result = sstable.find_block_with_fence_pointers("any_key".to_string());
        assert_eq!(result, None);
    }

    #[test]
    fn test_single_fence_pointer() {
        let fence_pointers = create_fence_pointers(vec!["m"]);
        let sstable = SSTable::new_for_tests(fence_pointers);

        // Any key should return block 0 since there's only one block
        let result = sstable.find_block_with_fence_pointers("a".to_string());
        assert_eq!(result, Some((0, 1)));

        let result = sstable.find_block_with_fence_pointers("m".to_string());
        assert_eq!(result, Some((0, 1)));

        let result = sstable.find_block_with_fence_pointers("z".to_string());
        assert_eq!(result, Some((0, 1)));
    }

    #[test]
    fn test_key_less_than_first_pointer() {
        let fence_pointers = create_fence_pointers(vec!["e", "j", "o", "t", "z"]);
        let sstable = SSTable::new_for_tests(fence_pointers);

        let result = sstable.find_block_with_fence_pointers("a".to_string());
        assert_eq!(result, Some((0, 1)));

        let result = sstable.find_block_with_fence_pointers("d".to_string());
        assert_eq!(result, Some((0, 1)));
    }

    #[test]
    fn test_key_greater_than_last_pointer() {
        let fence_pointers = create_fence_pointers(vec!["e", "j", "o", "t", "z"]);
        let sstable = SSTable::new_for_tests(fence_pointers);

        let result = sstable.find_block_with_fence_pointers("zz".to_string());
        assert_eq!(result, Some((4, 5))); // Should return index of the last fence pointer
    }

    #[test]
    fn test_key_exactly_matching_fence_pointer() {
        let fence_pointers = create_fence_pointers(vec!["e", "j", "o", "t", "z"]);
        let sstable = SSTable::new_for_tests(fence_pointers);

        // Should return the block containing the key
        let result = sstable.find_block_with_fence_pointers("e".to_string());
        assert_eq!(result, Some((0, 1)));

        let result = sstable.find_block_with_fence_pointers("j".to_string());
        assert_eq!(result, Some((1, 2)));

        let result = sstable.find_block_with_fence_pointers("o".to_string());
        assert_eq!(result, Some((2, 3)));
    }

    #[test]
    fn test_key_between_fence_pointers() {
        let fence_pointers = create_fence_pointers(vec!["e", "j", "o", "t", "z"]);
        let sstable = SSTable::new_for_tests(fence_pointers);

        // Should return the block where the key would be found
        let result = sstable.find_block_with_fence_pointers("g".to_string());
        assert_eq!(result, Some((0, 1))); // Between "e" and "j", should return 0

        let result = sstable.find_block_with_fence_pointers("l".to_string());
        assert_eq!(result, Some((1, 2))); // Between "j" and "o", should return 1

        let result = sstable.find_block_with_fence_pointers("r".to_string());
        assert_eq!(result, Some((2, 3))); // Between "o" and "t", should return 2

        let result = sstable.find_block_with_fence_pointers("w".to_string());
        assert_eq!(result, Some((3, 4))); // Between "t" and "z", should return 3
    }

    #[test]
    fn test_edge_cases() {
        let fence_pointers = create_fence_pointers(vec!["e", "j", "o", "t", "z"]);
        let sstable = SSTable::new_for_tests(fence_pointers);

        // Keys just before and after fence pointers
        let result = sstable.find_block_with_fence_pointers("i".to_string());
        assert_eq!(result, Some((0, 1))); // Just before "j", should return 0

        let result = sstable.find_block_with_fence_pointers("j".to_string());
        assert_eq!(result, Some((1, 2))); // Exact match "j", correctly return 1

        let result = sstable.find_block_with_fence_pointers("j0".to_string());
        assert_eq!(result, Some((1, 2))); // Just after "j", should return 1
    }

    #[test]
    fn test_case_sensitivity() {
        let fence_pointers = create_fence_pointers(vec!["A", "E", "M", "T", "Z"]);
        let sstable = SSTable::new_for_tests(fence_pointers);

        // Lowercase keys should be treated differently from uppercase (lexicographically)
        let result = sstable.find_block_with_fence_pointers("a".to_string());
        assert_eq!(result, Some((4, 5)));

        let result = sstable.find_block_with_fence_pointers("m".to_string());
        assert_eq!(result, Some((4, 5))); // Should match block 2, assuming "m" > "M" lexicographically
    }

    #[test]
    fn test_numeric_keys() {
        let fence_pointers = create_fence_pointers(vec!["1", "5", "10", "50", "100"]);
        let sstable = SSTable::new_for_tests(fence_pointers);

        // Lexicographical comparison for numeric strings
        let result = sstable.find_block_with_fence_pointers("2".to_string());
        assert_eq!(result, Some((4, 5))); // Between "1" and "5"

        let result = sstable.find_block_with_fence_pointers("20".to_string());
        assert_eq!(result, Some((4, 5))); // Between "10" and "50"
    }

    #[test]
    fn test_consecutive_identical_keys() {
        // Edge case: consecutive fence pointers with the same key
        let fence_pointers = vec![
            (Arc::from("a"), 0),
            (Arc::from("m"), 1),
            (Arc::from("m"), 2), // Duplicate key
            (Arc::from("z"), 3),
        ];

        let sstable = SSTable::new_for_tests(fence_pointers);

        let result = sstable.find_block_with_fence_pointers("m".to_string());
        // Should find the first "m" at index 1
        assert_eq!(result, Some((1, 2)));
    }

    // This test ensures we correctly handle the upper_bound calculation
    #[test]
    fn test_upper_bound_calculation() {
        // Test with exactly 2 fence pointers
        let fence_pointers = create_fence_pointers(vec!["a", "z"]);
        let sstable = SSTable::new_for_tests(fence_pointers);

        // Should still work properly with just 2 pointers
        let result = sstable.find_block_with_fence_pointers("m".to_string());
        assert_eq!(result, Some((0, 1)));
    }
}
