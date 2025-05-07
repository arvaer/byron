use crate::lsm_compaction::Monkey;
use key_value::KeyValue;
use memtable::{mem_table_builder::MemTableBuilder, MemTable, MemTableOperations};
use sstable::{builder::SSTableFeatures, error::SSTableError, SSTable};
use std::{mem, path::PathBuf, sync::Arc, thread};
use uuid::Uuid;

use crate::error::LsmError;

#[derive(Debug)]
pub struct Level {
    pub inner: Vec<Arc<SSTable>>,
    pub depth: usize,
    pub width: usize,
    pub total_entries: usize,
}

#[derive(Debug)]
pub struct LsmDatabase {
    pub primary: MemTable,
    pub tables: Vec<Arc<SSTable>>,
    pub capacity_expansion_factor: f64,
    pub parent_directory: PathBuf,
    pub levels: Vec<Level>,
    pub base_fpr: f64,
}

impl Default for LsmDatabase {
    fn default() -> Self {
        Self {
            primary: MemTable::default(),
            tables: Vec::new(),
            parent_directory: PathBuf::from("./data"),
            capacity_expansion_factor: 1.618,
            levels: Vec::new(),
            base_fpr: 0.005,
        }
    }
}

impl LsmDatabase {
    pub fn new(parent_directory: String, capacity_expansion_factor: Option<f64>) -> Self {
        let first_level = Level {
            inner: Vec::new(),
            depth: 0,
            width: 2,
            total_entries: 0,
        };
        Self {
            primary: MemTableBuilder::default().max_entries(1000).build(),
            tables: Vec::new(),
            parent_directory: parent_directory.into(),
            capacity_expansion_factor: capacity_expansion_factor.unwrap_or(1.618),
            levels: vec![first_level],
            base_fpr: 0.005,
        }
    }

    pub fn flash(&mut self) -> std::thread::JoinHandle<Arc<SSTable>> {
        let mut old_table = mem::replace(
            &mut self.primary,
            MemTableBuilder::default().max_entries(10).build(),
        );

        let parent_directory = self.parent_directory.clone();
        let features = self.calculate_sstable_features(old_table.current_length());

        thread::spawn(move || {
            let path = parent_directory.join(format!("sstable-id-{}", Uuid::new_v4()));
            old_table
                .flush(path, features)
                .expect("Failed to flush memtable")
        })
    }

    pub fn calculate_sstable_features(&self, item_count: usize) -> SSTableFeatures {
        SSTableFeatures {
            item_count,
            fpr: 0.01,
        }
    }

    pub fn get(&self, key: String) -> Result<Arc<KeyValue>, LsmError> {
        if let Some(kv) = self.primary.get(&key) {
            if kv.value == "deadbeef" {
                return Err(LsmError::KeyNotFound);
            }
            return Ok(kv.into());
        }
        for level in self.levels.iter() {
            for sstable in level.inner.iter() {
                match sstable.get(key.clone()) {
                    Ok(kv) => {
                        if kv.value == "d34b33f" {
                            return Err(LsmError::KeyNotFound);
                        }
                        return Ok(kv);
                    }
                    Err(SSTableError::KeyNotfound) => continue,
                    Err(e) => return Err(LsmError::SSTable(e)),
                }
            }
        }

        Err(LsmError::KeyNotFound)
    }

    pub fn put(&mut self, key: String, value: String) -> Result<(), LsmError> {
        self.primary.put(key, value);
        if self.primary.at_capacity() {
            let sstable = self.flash().join().expect("Flushing thread panicked");
            self.insert_new_table(sstable, 0)?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: String) -> Result<(), LsmError> {
        let sentinel = String::from("d34db33f");
        self.put(key, sentinel)?;
        Ok(())
    }

    pub fn range(&self, from_m: String, to_n: String) -> Result<Vec<Box<KeyValue>>, LsmError> {
        let (mut results, flag) = self.primary.range(&from_m.clone(), &to_n.clone());

        match flag {
            memtable::RangeResult::FullSetFound => {
                return Ok(results);
            }
            memtable::RangeResult::FirstKeyFound => {
                let start_key = results
                    .last()
                    .map(|kv| kv.key.clone())
                    .unwrap_or_else(|| from_m.clone());

                'outer: for level in &self.levels {
                    for sstable in &level.inner {
                        let (sst_entries, found_end) =
                            sstable.get_until(&to_n).map_err(LsmError::SSTable)?;

                        for box_kv in sst_entries {
                            let k = &box_kv.key;
                            if k.as_str() > start_key.as_str() && k.as_str() >= from_m.as_str() {
                                results.push(box_kv);
                            }
                        }

                        if found_end {
                            break 'outer;
                        }
                    }
                }
            }
            memtable::RangeResult::KeyNotFound => {
                'outer: for level in &self.levels {
                    for sstable in &level.inner {
                        match sstable.get(from_m.clone()) {
                            Ok(_) => {
                                for kv_res in sstable.iter() {
                                    let kv = kv_res.map_err(LsmError::SSTable)?;

                                    if kv.key < from_m {
                                        continue;
                                    }
                                    if kv.key > to_n {
                                        break 'outer;
                                    }

                                    results.push(Box::new(kv.clone()));
                                }
                                break 'outer;
                            }
                            Err(SSTableError::KeyNotfound) => continue,
                            Err(e) => return Err(LsmError::SSTable(e)),
                        }
                    }
                }
            }
        }
        if results.is_empty() {
            Err(LsmError::KeyNotFound)
        } else {
            Ok(results)
        }
    }
}

#[cfg(test)]
mod lsm_database_tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_db() -> (LsmDatabase, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap().to_string();
        let mut db = LsmDatabase::new(path, None);
        db.primary = MemTableBuilder::default().max_entries(5).build();
        (db, temp_dir)
    }

    fn create_kv(prefix: &str, idx: usize) -> (String, String) {
        let key = format!("{}-key-{}", prefix, idx);
        let value = format!("{}-value-{}", prefix, idx);
        (key, value)
    }

    #[test]
    fn test_basic_put_get() {
        let (mut db, _temp_dir) = create_test_db();
        let (key, value) = create_kv("test", 1);
        db.put(key.clone(), value.clone()).unwrap();
        let result = db.get(key.clone()).unwrap();
        assert_eq!(result.key, key);
        assert_eq!(result.value, value);
    }

    #[test]
    fn test_memtable_flush() {
        let (mut db, _temp_dir) = create_test_db();
        for i in 0..1000 {
            let (key, value) = create_kv("flush", i);
            db.put(key, value).unwrap();
        }

        assert!(!db.levels[0].inner.is_empty() || !db.levels[1].inner.is_empty());
        // SSTable should be in level 0 or 1
    }

    #[test]
    fn test_simple_compaction() {
        let (mut db, _temp_dir) = create_test_db();

        for i in 0..21 {
            let (key, value) = create_kv("compaction", i);
            db.put(key, value).unwrap();
        }

        assert!(db.levels.len() > 1);

        for i in 0..20 {
            let (key, value) = create_kv("compaction", i);
            let result = db.get(key.clone());
            assert!(result.is_ok(), "Failed to retrieve key: {}", key);
            assert_eq!(result.unwrap().value, value);
        }
    }

    #[test]
    fn test_multi_level_compaction() {
        let (mut db, _temp_dir) = create_test_db();
        for i in 0..100 {
            let (key, value) = create_kv("multilevel", i);
            db.put(key, value).unwrap();
        }

        // Verify that we have at least 3 levels
        assert!(
            db.levels.len() >= 3,
            "Expected at least 3 levels, got {}",
            db.levels.len()
        );

        // Check if we can still retrieve all keys after multi-level compaction
        for i in 0..100 {
            let (key, value) = create_kv("multilevel", i);
            let result = db.get(key.clone());
            assert!(result.is_ok(), "Failed to retrieve key: {}", key);
            assert_eq!(result.unwrap().value, value);
        }
    }

    #[test]
    fn test_update_existing_keys() {
        let (mut db, _temp_dir) = create_test_db();

        // Insert keys
        for i in 0..3 {
            let (key, value) = create_kv("update", i);
            db.put(key, value).unwrap();
        }

        // Update the same keys with new values
        for i in 0..3 {
            let (key, _) = create_kv("update", i);
            let new_value = format!("updated-value-{}", i);
            db.put(key, new_value).unwrap();
        }

        // Verify that we have the updated values
        for i in 0..3 {
            let (key, _) = create_kv("update", i);
            let expected_value = format!("updated-value-{}", i);
            let result = db.get(key.clone()).unwrap();
            assert_eq!(result.value, expected_value);
        }
    }

    #[test]
    fn test_interleaved_put_get_with_compaction() {
        let (mut db, _temp_dir) = create_test_db();

        // Phase 1: Insert some keys
        for i in 0..10 {
            let (key, value) = create_kv("interleaved", i);
            db.put(key, value).unwrap();
        }

        // Verify all keys from phase 1
        for i in 0..10 {
            let (key, value) = create_kv("interleaved", i);
            let result = db.get(key).unwrap();
            assert_eq!(result.value, value);
        }

        // Phase 2: Insert more keys to trigger compaction
        for i in 10..30 {
            let (key, value) = create_kv("interleaved", i);
            db.put(key, value).unwrap();
        }

        // Verify all keys from phase 1 and 2
        for i in 0..30 {
            let (key, value) = create_kv("interleaved", i);
            let result = db.get(key).unwrap();
            assert_eq!(result.value, value);
        }

        // Phase 3: Insert even more keys for multi-level compaction
        for i in 30..60 {
            let (key, value) = create_kv("interleaved", i);
            db.put(key, value).unwrap();
        }

        // Verify all keys are still accessible
        for i in 0..60 {
            let (key, value) = create_kv("interleaved", i);
            let result = db.get(key).unwrap();
            assert_eq!(result.value, value);
        }
    }

    #[test]
    fn test_lookup_nonexistent_key() {
        let (mut db, _temp_dir) = create_test_db();

        // Insert some keys
        for i in 0..20 {
            let (key, value) = create_kv("exists", i);
            db.put(key, value).unwrap();
        }

        // Look up a key that doesn't exist
        let nonexistent_key = "nonexistent-key".to_string();
        let result = db.get(nonexistent_key);
        assert!(result.is_err());

        if let Err(err) = result {
            match err {
                LsmError::KeyNotFound => { /* expected error */ }
                _ => panic!("Expected KeyNotFound error, got: {:?}", err),
            }
        }
    }
}
