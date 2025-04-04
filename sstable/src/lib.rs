use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Cursor, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};

use block_iter::SSTableBlockIterator;
use bloomfilter::Bloom;
use chained_blocks::SSTableIterator;
use error::SSTableError;
use integer_encoding::VarIntReader;
use key_value::{key_value_pair::DeltaEncodedKV, KeyValue};

mod block_iter;
pub mod builder;
pub mod streamed_builder;
mod chained_blocks;
pub mod error;
mod operations;

#[derive(Debug)]
pub struct SSTable {
    file_path: PathBuf,
    fd: Option<File>,
    page_hash_indices: Vec<HashMap<String, usize>>, // One hash index per block
    fence_pointers: Vec<(Arc<str>, usize)>,
    restart_indices: Vec<Vec<usize>>, // Restart indices for each block
    bloom_filter: Arc<Bloom<String>>,
    actual_item_count: usize
}

impl SSTable {
    pub fn get(&self, key: String) -> Result<Arc<KeyValue>, SSTableError> {
        if !self.bloom_filter.check(&key) {
            return Err(SSTableError::KeyNotfound);
        }

        let block_idx = self
            .find_block_with_fence_pointers(key.clone())
            .unwrap_or((0, 1));

        let block_data = self.read_block_from_disk(block_idx)?;

        if block_idx.0 >= self.restart_indices.len() || block_idx.0 >= self.page_hash_indices.len()
        {
            return Err(SSTableError::KeyNotfound);
        }

        let block_start = if block_idx.0 < self.fence_pointers.len() {
            self.fence_pointers[block_idx.0].1
        } else {
            4 // Fallback (right after header which is SSTB)
        };

        let restart_points = self.restart_indices[block_idx.0].clone();

        if let Some(position) = self.page_hash_indices[block_idx.0].get(&key) {
            if *position >= restart_points.len() {
                return Err(SSTableError::KeyNotfound);
            }

            let restart_point = restart_points[*position];
            let read_size = 4096 / 16;

            if restart_point >= block_data.len() {
                return Err(SSTableError::KeyNotfound);
            }

            let end_pos = std::cmp::min(restart_point + read_size, block_data.len());
            let kvps = &block_data[restart_point..end_pos];

            let kvp: Arc<KeyValue> = match self.deserialize_run_get_key(kvps, key.clone()) {
                Ok(key) => Arc::new(key),
                Err(SSTableError::KVPexceedsBlock(e)) => {
                    let fk = self.deserialize_spanning_key(e + block_start, key)?;
                    return Ok(Arc::new(fk));
                }
                Err(_) => return Err(SSTableError::KeyNotfound),
            };
            return Ok(kvp);
        }

        let kvp: Arc<KeyValue> = match self.linear_search(block_data, key.clone(), &restart_points)
        {
            Ok(key) => Arc::new(key),
            Err(SSTableError::KVPexceedsBlock(e)) => {
                let fk = self.deserialize_spanning_key(e + block_start, key.clone())?;
                return Ok(Arc::new(fk));
            }
            Err(_) => return Err(SSTableError::KeyNotfound),
        };
        Ok(kvp)
    }

    fn deserialize_spanning_key(
        &self,
        start_offset: usize,
        needle: String,
    ) -> Result<KeyValue, SSTableError> {
        let mut file = if let Some(ref f) = self.fd {
            f.try_clone().map_err(SSTableError::FileSystemError)?
        } else {
            File::open(&self.file_path).map_err(SSTableError::FileSystemError)?
        };

        file.seek(SeekFrom::Start(start_offset as u64))
            .map_err(SSTableError::FileSystemError)?;

        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();

        loop {
            let mut chunk = [0u8; 4096];
            let n = reader
                .read(&mut chunk)
                .map_err(SSTableError::FileSystemError)?;
            if n == 0 {
                log::info!("Broke the key!! D:");
                break;
            }
            buffer.extend_from_slice(&chunk[..n]);

            match self.deserialize_run_get_key(&buffer, needle.clone()) {
                Ok(kv) => return Ok(kv),
                Err(SSTableError::KVPexceedsBlock(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(SSTableError::KeyNotfound)
    }

    fn deserialize_run_get_key(
        &self,
        run: &[u8],
        needle: String,
    ) -> Result<KeyValue, SSTableError> {
        // Return None if run is empty
        if run.is_empty() {
            return Err(SSTableError::KeyNotfound);
        }

        let mut cursor = Cursor::new(run);
        let mut previous_key: Option<KeyValue> = None;

        while (cursor.position() as usize) < run.len() {
            let remaining_size = run.len() - cursor.position() as usize;
            if remaining_size <= 1 {
                return Err(SSTableError::KVPexceedsBlock(cursor.position() as usize));
            }

            let shared_bytes = match cursor.read_varint() {
                Ok(val) => val,
                Err(_) => break,
            };
            let unshared_bytes = match cursor.read_varint() {
                Ok(val) => val,
                Err(_) => break,
            };
            if remaining_size <= unshared_bytes {
                return Err(SSTableError::KVPexceedsBlock(cursor.position() as usize));
            }

            let value_bytes = match cursor.read_varint() {
                Ok(val) => val,
                Err(_) => break,
            };
            if remaining_size <= value_bytes {
                return Err(SSTableError::KVPexceedsBlock(cursor.position() as usize));
            }

            if unshared_bytes > run.len() - cursor.position() as usize
                || value_bytes > run.len() - cursor.position() as usize - unshared_bytes
            {
                return Err(SSTableError::KVPexceedsBlock(cursor.position() as usize));
            }

            let mut key_delta = vec![0u8; unshared_bytes];
            if cursor.read_exact(&mut key_delta).is_err() {
                break;
            }

            let mut value = vec![0u8; value_bytes];
            if cursor.read_exact(&mut value).is_err() {
                break;
            }

            let dkv = DeltaEncodedKV {
                shared_bytes,
                unshared_bytes,
                value_bytes,
                key_delta: key_delta.into_boxed_slice(),
                value: value.into_boxed_slice(),
            };

            let as_key = match dkv.reverse(previous_key) {
                Some(kv) => kv,
                None => break,
            };

            if as_key.key == needle {
                return Ok(as_key);
            }
            previous_key = Some(as_key);
        }

        Err(SSTableError::KeyNotfound)
    }

    fn deserialize_first_key_from_run(&self, run: &[u8]) -> Option<KeyValue> {
        if run.is_empty() {
            return None;
        }

        let mut cursor = Cursor::new(run);

        let shared_bytes = match cursor.read_varint() {
            Ok(val) => val,
            Err(_) => return None,
        };

        if shared_bytes != 0 {
            log::info!("WARNING: First key shared byte is not 0. You are probably mangling keys!");
        }

        let unshared_bytes = match cursor.read_varint() {
            Ok(val) => val,
            Err(_) => return None,
        };

        let value_bytes = match cursor.read_varint() {
            Ok(val) => val,
            Err(_) => return None,
        };

        log::info!(
            "DEBUG: First key metadata: shared_bytes={}, unshared_bytes={}, value_bytes={}",
            shared_bytes,
            unshared_bytes,
            value_bytes
        );
        if unshared_bytes > run.len() || value_bytes > run.len() {
            return None;
        }

        let mut key_delta = vec![0u8; unshared_bytes];
        if cursor.read_exact(&mut key_delta).is_err() {
            return None;
        }

        let mut value = vec![0u8; value_bytes];
        if cursor.read_exact(&mut value).is_err() {
            return None;
        }

        let dkv = DeltaEncodedKV {
            shared_bytes,
            unshared_bytes,
            value_bytes,
            key_delta: key_delta.into_boxed_slice(),
            value: value.into_boxed_slice(),
        };

        dkv.reverse(None)
    }

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

    fn read_block_from_disk(&self, block_idx: (usize, usize)) -> Result<Arc<[u8]>, SSTableError> {
        let mut reader: BufReader<&File>;
        let file: File;
        if let Some(file) = &self.fd {
            reader = BufReader::new(file);
        } else {
            file = File::open(&self.file_path).map_err(SSTableError::FileSystemError)?;
            reader = BufReader::new(&file);
        }

        log::info!("DEBUG: Reading block with index: {:?}", block_idx);

        if block_idx.0 == 0 {
            // we have a magic SSTB at the start
            reader.seek(SeekFrom::Start(0))?;
            let mut magic = [0u8; 4];
            reader.read_exact(&mut magic)?;

            if &magic != b"SSTB" {
                return Err(SSTableError::FileSystemError(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid file magic",
                )));
            }
        } else {
            reader.seek(SeekFrom::Start(4))?;
        }

        let start_offset = if block_idx.0 < self.fence_pointers.len() {
            self.fence_pointers[block_idx.0].1
        } else {
            4 // right after head
        };

        let end_offset = if block_idx.1 < self.fence_pointers.len() {
            self.fence_pointers[block_idx.1].1
        } else {
            std::fs::metadata(&self.file_path)?.len() as usize - 4 // Before the Footer
        };

        let block_size = end_offset - start_offset;
        log::info!(
            "DEBUG: Reading from offset {} to {}, size {}",
            start_offset,
            end_offset,
            block_size
        );

        reader.seek(SeekFrom::Start(start_offset as u64))?;
        let mut block_data = vec![0u8; block_size];
        reader.read_exact(&mut block_data)?;

        log::info!("DEBUG: Successfully read {} bytes", block_data.len());
        if !block_data.is_empty() {
            let display_size = std::cmp::min(block_data.len(), 50);
            log::info!(
                "DEBUG: First {} bytes: {:?}",
                display_size,
                &block_data[..display_size]
            );
        }

        Ok(Arc::from(block_data.into_boxed_slice()))
    }

    fn linear_search(
        &self,
        block_data: Arc<[u8]>,
        key: String,
        restart_points: &[usize],
    ) -> Result<KeyValue, SSTableError> {
        // Try linear scan first for debugging
        log::info!("DEBUG: defaulting to linear scan for now");
        let kv = self.deserialize_run_get_key(&block_data, key.clone());
        match kv {
            Ok(kv) => return Ok(kv),
            Err(SSTableError::KVPexceedsBlock(e)) => return Err(SSTableError::KVPexceedsBlock(e)),
            _ => {}
        }
        // This is dead code hopefully. its a failed attempt at bsearching the restart pointers
        log::info!("WARNING-- YOU RELALY SHOUDLNT BE HERE");
        log::info!("DEBUG: Binary searching for key: '{}'", key);
        log::info!("DEBUG: Block data size: {} bytes", block_data.len());
        log::info!("DEBUG: Restart points: {:?}", restart_points);
        log::info!("DEBUG: Binary search complete, key not found");
        // Handle empty restart points
        if restart_points.is_empty() {
            log::info!("DEBUG: No restart points available");
            return Err(SSTableError::KeyNotfound);
        }

        let mut left = 0;
        let mut right = restart_points.len();

        let max_block_size = block_data.len();
        let run_size = 4096 / 16; // Keep your constant if it's intentional

        log::info!("DEBUG: Using run size: {}", run_size);

        while left < right {
            let mid = left + (right - left) / 2;
            log::info!(
                "DEBUG: Checking restart point {} ({} to {})",
                mid,
                left,
                right
            );

            if mid >= restart_points.len() {
                log::info!("DEBUG: Mid index out of bounds");
                break;
            }

            let restart_pos = restart_points[mid];
            log::info!("DEBUG: Restart position: {}", restart_pos);

            let start_pos = std::cmp::min(restart_pos, max_block_size.saturating_sub(1));
            let end_pos = std::cmp::min(start_pos + run_size, max_block_size);
            log::info!("DEBUG: Checking range {} to {}", start_pos, end_pos);

            if start_pos >= end_pos {
                log::info!("DEBUG: Invalid range (start >= end)");
                break;
            }

            let run = &block_data[start_pos..end_pos];
            log::info!("DEBUG: Run size: {} bytes", run.len());
            if !run.is_empty() {
                log::info!(
                    "DEBUG: First few bytes: {:?}",
                    &run[..std::cmp::min(10, run.len())]
                );
            }

            if let Ok(key) = self.deserialize_run_get_key(run, key.clone()) {
                return Ok(key);
            }

            if let Some(keyvalue) = self.deserialize_first_key_from_run(run) {
                log::info!("DEBUG: First key in run: '{}'", keyvalue.key);
                if keyvalue.key > key {
                    log::info!("DEBUG: First key > target key, moving left");
                    right = mid;
                } else {
                    log::info!("DEBUG: First key <= target key, moving right");
                    left = mid + 1;
                }
            } else {
                log::info!("DEBUG: Failed to deserialize first key from run");
                left = mid + 1;
            }
        }
        Err(SSTableError::KeyNotfound)
    }

    pub fn iter_block(
        &self,
        block_idx: (usize, usize),
    ) -> Result<SSTableBlockIterator, SSTableError> {
        let block_data = self.read_block_from_disk(block_idx)?.clone();

        Ok(SSTableBlockIterator::new(block_data))
    }
    pub fn iter(&self) -> SSTableIterator {
        SSTableIterator::new(self)
    }
}

impl <'a> IntoIterator for &'a SSTable {
    type Item = Result<KeyValue, SSTableError>;
    type IntoIter = SSTableIterator<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use builder::{SSTableBuilder, SSTableFeatures};
    use key_value::KeyValue;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn create_test_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    impl SSTable {
        fn new_for_tests(fence_pointers: Vec<(Arc<str>, usize)>) -> Self {
            // create a minimal valid bloom filter for testing
            //let bloom = Bloom::new_for_fp_rate(100, 0.01).unwrap();

            Self {
                file_path: PathBuf::from("test.sst"),
                fd: None,
                page_hash_indices: Vec::new(),
                fence_pointers,
                restart_indices: Vec::new(),
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
        let fence_pointers = create_fence_pointers(vec!["a", "z"]);
        let sstable = SSTable::new_for_tests(fence_pointers);
        let result = sstable.find_block_with_fence_pointers("m".to_string());
        assert_eq!(result, Some((0, 1)));
    }

    #[test]
    fn test_get_existing_key() -> Result<(), SSTableError> {
        log::info!("========= STARTING TEST_GET_EXISTING_KEY =========");
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_get_key.sst");

        log::info!("Using file path: {:?}", file_path);

        let features = SSTableFeatures {
            lz: false,
            fpr: 0.01,
        };

        log::info!("Creating SSTableBuilder...");
        let mut builder = SSTableBuilder::new(features, &file_path, 100)?;

        let test_key = "test-key-42";
        let test_value = "test-value-42";

        log::info!("Adding test key: {} with value: {}", test_key, test_value);
        builder.add_from_kv(create_test_kv(test_key, test_value))?;

        log::info!("Adding other keys...");
        for i in 0..5 {
            let key = format!("other-key-{}", i);
            let value = format!("other-value-{}", i);
            log::info!("  Adding: {} = {}", key, value);
            builder.add_from_kv(create_test_kv(&key, &value))?;
        }

        log::info!("Building SSTable...");
        let sstable = builder.build()?;

        log::info!("SSTable built. Stats:");
        log::info!("  Fence pointers count: {}", sstable.fence_pointers.len());
        log::info!("  Restart indices count: {}", sstable.restart_indices.len());
        log::info!(
            "  Page hash indices count: {}",
            sstable.page_hash_indices.len()
        );

        if !sstable.fence_pointers.is_empty() {
            log::info!("Fence pointers:");
            for (i, (key, offset)) in sstable.fence_pointers.iter().enumerate() {
                log::info!("  [{}] Key: '{}', Offset: {}", i, key, offset);
            }
        }

        if !sstable.restart_indices.is_empty() {
            log::info!("Restart indices (first block):");
            for (i, pos) in sstable.restart_indices[0].iter().enumerate() {
                log::info!("  [{}] Position: {}", i, pos);
            }
        }

        if !sstable.page_hash_indices.is_empty() {
            log::info!("Page hash indices (first block):");
            for (key, pos) in sstable.page_hash_indices[0].iter() {
                log::info!("  Key: '{}', Position: {}", key, pos);
            }
        }
        log::info!("checking if file exists: {}", file_path.exists());
        assert!(file_path.exists(), "sstable file was not created");

        let file_size = std::fs::metadata(&file_path)?.len();
        log::info!("file size: {} bytes", file_size);

        log::info!("checking if key '{}' is in bloom filter...", test_key);

        log::info!("finding block with fence pointers...");
        let block_idx = sstable
            .find_block_with_fence_pointers(test_key.to_string())
            .unwrap_or((0, 1));
        log::info!("block index: {:?}", block_idx);

        let block_data_result = sstable.read_block_from_disk(block_idx);
        match &block_data_result {
            Ok(data) => log::info!("block data size: {} bytes", data.len()),
            Err(e) => log::info!("error reading block: {:?}", e),
        }

        let block_data = block_data_result?;

        log::info!("checking restart indices for block {}...", block_idx.0);
        if block_idx.0 < sstable.restart_indices.len() {
            let restart_points = &sstable.restart_indices[block_idx.0];
            log::info!("restart points count: {}", restart_points.len());
            for (i, pos) in restart_points.iter().enumerate() {
                log::info!("  [{}] Pos: {}", i, pos);
            }
        } else {
            log::info!("block index out of bounds for restart_indices");
        }

        log::info!("checking page hash indices for block {}...", block_idx.0);
        if block_idx.0 < sstable.page_hash_indices.len() {
            let page_hash = &sstable.page_hash_indices[block_idx.0];
            log::info!("page hash count: {}", page_hash.len());
            log::info!("does page has key? {}", page_hash.contains_key(test_key));

            if let Some(pos) = page_hash.get(test_key) {
                log::info!("position in page hash: {}", pos);
            }
        } else {
            log::info!("Block iob 4 page_hash_indices");
        }
        log::info!("attempting to get key: '{}'::", test_key);
        match sstable.get(test_key.to_string()) {
            Ok(kv) => {
                log::info!("success! key found!");
                log::info!("retrieved value: '{}'", kv.value);
                assert_eq!(kv.value, test_value);
                Ok(())
            }
            Err(e) => {
                log::info!("failed to retrieve key: {:?}", e);
                Err(e)
            }
        }
    }

    #[test]
    fn test_get_nonexistent_key() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.sst");

        let features = SSTableFeatures {
            lz: false,
            fpr: 0.01,
        };

        let mut builder = SSTableBuilder::new(features, &file_path, 100)?;

        for i in 0..100 {
            let key = format!("key-{:05}", i);
            builder.add_from_kv(create_test_kv(&key, &format!("value-{}", i)))?;
        }

        let sstable = builder.build()?;

        // Test getting a nonexistent key
        let key = "nonexistent-key".to_string();
        let result = sstable.get(key);
        assert!(matches!(result, Err(SSTableError::KeyNotfound)));

        Ok(())
    }

    #[test]
    fn test_binary_search_with_restarts() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.sst");

        let features = SSTableFeatures {
            lz: false,
            fpr: 0.01,
        };

        let mut builder = SSTableBuilder::new(features, &file_path, 1000)?;

        for i in 0..250 {
            let key = format!("key-{:05}", i);
            builder.add_from_kv(create_test_kv(&key, "5"))?;
        }

        let sstable = builder.build()?;

        let test_keys = [
            "key-00000", // First key
            "key-00050", // Middle key
            "key-00075", // Random key
            "key-00199", // Last key
        ];

        for &key in &test_keys {
            let result = sstable.get(key.to_string())?;
            log::info!("result: {:?}", result);
            assert_eq!(result.value, "5");
        }

        Ok(())
    }

    #[test]
    fn test_page_hash_index() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.sst");

        let features = SSTableFeatures {
            lz: false,
            fpr: 0.01,
        };

        let mut builder = SSTableBuilder::new(features, &file_path, 1000)?;

        for i in 0..500 {
            let key = format!("key-{:05}", i);
            let value = "x".repeat(100); // Large values to force multiple blocks
            builder.add_from_kv(create_test_kv(&key, &value))?;
        }

        let sstable = builder.build()?;

        assert!(!sstable.page_hash_indices.is_empty());

        for i in (0..500).step_by(50) {
            let key = format!("key-{:05}", i);
            let result = sstable.get(key.to_string()).unwrap();
            assert!(result.key == key);
        }

        Ok(())
    }

    #[test]
    fn test_multiple_blocks_edge_cases() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.sst");

        let features = SSTableFeatures {
            lz: false,
            fpr: 0.01,
        };

        let mut builder = SSTableBuilder::new(features, &file_path, 1000)?;

        // add keys that force block boundaries
        for i in 0..150 {
            let key = format!("key-{:05}", i);
            let value = "x".repeat(if i % 50 == 0 { 1000 } else { 10 }); // Force block splits at every 50th key
            builder.add_from_kv(create_test_kv(&key, &value))?;
        }

        builder.build()?;

        let sstable = SSTable {
            file_path: file_path.clone(),
            fd: None,
            page_hash_indices: builder.page_hash_indices.clone(),
            fence_pointers: builder.fence_pointers.clone(),
            restart_indices: builder.restart_indices.clone(),
        };

        for i in (0..150).step_by(50) {
            let key = format!("key-{:05}", i);
            let result = sstable.get(key);
            assert!(result.is_ok());
        }
        for i in [49, 50, 51, 99, 100, 101] {
            let key = format!("key-{:05}", i);
            let result = sstable.get(key);
            assert!(result.is_ok());
        }

        Ok(())
    }

    #[test]
    fn test_read_block_from_disk() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.sst");

        let features = SSTableFeatures {
            lz: false,
            fpr: 0.01,
        };

        let mut builder = SSTableBuilder::new(features, &file_path, 100)?;
        for i in 0..50 {
            let key = format!("key-{:05}", i);
            builder.add_from_kv(create_test_kv(&key, &format!("value-{}", i)))?;
        }

        builder.build()?;

        let sstable = SSTable {
            file_path: file_path.clone(),
            fd: None, // Testing with closed file
            page_hash_indices: builder.page_hash_indices.clone(),
            fence_pointers: builder.fence_pointers.clone(),
            restart_indices: builder.restart_indices.clone(),
        };

        if !sstable.fence_pointers.is_empty() {
            let block_offset = (0, 1); // First block
            let block_data = sstable.read_block_from_disk(block_offset)?;
            assert!(!block_data.is_empty());
        }

        Ok(())
    }

    #[test]
    fn test_concurrent_reads() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.sst");

        let features = SSTableFeatures {
            lz: false,
            fpr: 0.01,
        };

        let mut builder = SSTableBuilder::new(features, &file_path, 1000)?;

        for i in 0..200 {
            let key = format!("key-{:05}", i);
            builder.add_from_kv(create_test_kv(&key, &format!("value-{}", i)))?;
        }

        let sstable = builder.build()?;
        let sstable_arc = Arc::new(sstable);

        let mut handles = vec![];
        for t in 0..10 {
            let sstable_clone = Arc::clone(&sstable_arc);
            let handle = std::thread::spawn(move || {
                let start = t * 20;
                let end = start + 20;

                for i in start..end {
                    let key = format!("key-{:05}", i);
                    let result = sstable_clone.get(key);
                    assert!(result.is_ok());
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        Ok(())
    }

    #[test]
    fn test_large_values() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.sst");

        let features = SSTableFeatures {
            lz: false,
            fpr: 0.01,
        };

        let mut builder = SSTableBuilder::new(features, &file_path, 100)?;

        let large_value = "x".repeat(1_000_000); // 1MB value
        builder.add_from_kv(create_test_kv("large-key", &large_value))?;

        let sstable = builder.build()?;
        let result = sstable.get("large-key".to_string())?;
        assert_eq!(result.value.len(), 1_000_000);

        Ok(())
    }

    #[test]
    fn test_high_cardinality_prefixes() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.sst");

        let features = SSTableFeatures {
            lz: false,
            fpr: 0.01,
        };

        let mut builder = SSTableBuilder::new(features, &file_path, 1000)?;

        // Add keys with high-cardinality prefixes to test delta encoding
        for i in 0..100 {
            // Different prefixes for each key
            let key = format!("prefix-{:03}:suffix-{:03}", i % 10, i);
            builder.add_from_kv(create_test_kv(&key, &format!("value-{}", i)))?;
        }

        let sstable = builder.build()?;

        // Test a sampling of keys
        for i in [0, 15, 35, 67, 99] {
            let key = format!("prefix-{:03}:suffix-{:03}", i % 10, i);
            let result = sstable.get(key)?;
            assert_eq!(result.value, format!("value-{}", i));
        }

        Ok(())
    }

    #[test]
    fn test_sorted_insertion() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.sst");

        let features = SSTableFeatures {
            lz: false,
            fpr: 0.01,
        };

        let mut builder = SSTableBuilder::new(features, &file_path, 100)?;
        let mut keys = Vec::new();
        for i in (0..50).rev() {
            let key = format!("key-{:05}", i);
            keys.push(key.clone());
            builder.add_from_kv(create_test_kv(&key, &format!("value-{}", i)))?;
        }

        let sstable = builder.build()?;
        keys.sort();

        if !sstable.fence_pointers.is_empty() {
            let mut fence_keys = sstable
                .fence_pointers
                .iter()
                .map(|(k, _)| k.to_string())
                .collect::<Vec<_>>();

            fence_keys.sort();

            for i in 0..fence_keys.len() {
                assert_eq!(sstable.fence_pointers[i].0.to_string(), fence_keys[i]);
            }
        }

        Ok(())
    }
}
