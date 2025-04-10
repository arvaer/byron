use crate::wall_e::CompactionResult;
use crate::wall_e::Walle;
use key_value::KeyValue;
use memtable::{mem_table_builder::MemTableBuilder, MemTable, MemTableOperations};
use sstable::{builder::SSTableFeatures, error::SSTableError, SSTable};
use std::{collections::HashMap, mem, path::PathBuf, sync::Arc};
use uuid::Uuid;

use crate::error::LsmError;

#[derive(Debug, Clone)]
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
    pub wall_e: Walle,
    pub pending_compactions: HashMap<usize, usize>,
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
            wall_e: Walle::new(),
            pending_compactions: HashMap::new(),
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
            primary: MemTableBuilder::default().max_entries(10).build(),
            tables: Vec::new(),
            parent_directory: parent_directory.into(),
            capacity_expansion_factor: capacity_expansion_factor.unwrap_or(1.618),
            levels: vec![first_level],
            base_fpr: 0.005,
            wall_e: Walle::new(),
            pending_compactions: HashMap::new(),
        }
    }

    fn flash(&mut self) -> tokio::task::JoinHandle<Arc<SSTable>> {
        let mut old_table = mem::replace(
            &mut self.primary,
            MemTableBuilder::default().max_entries(10).build(),
        );

        let parent_directory = self.parent_directory.clone();
        let features = self.calculate_sstable_features(old_table.current_length());

        tokio::task::spawn(async move {
            let path = parent_directory.join(format!("sstable-id-{}", Uuid::new_v4()));
            old_table
                .flush(path, features)
                .expect("Failed to flush memtable")
        })
    }

    pub fn calculate_sstable_features(&self, item_count: usize) -> SSTableFeatures {
        SSTableFeatures {
            item_count,
            fpr: self.base_fpr,
        }
    }

    async fn check_for_compactions(&mut self) -> Result<(), LsmError> {
        let compactions = self.wall_e.drain_results().await;
        for pending in compactions{
            match pending {
                CompactionResult::Completed {
                    compacted_table,
                    original_level,
                    items_processed: _,
                } => {
                    if let Some(entry) = self.pending_compactions.get_mut(&original_level) {
                        *entry -= 1;
                        if *entry == 0 {
                            self.pending_compactions.remove(&original_level);
                        }
                    }

                    let target_level = original_level + 1;
                    println!("Inserting from check_for_compactions");

                    let level = &mut self.levels[original_level];
                    level.inner.clear();
                    level.total_entries = 0;

                    self.insert_new_table(compacted_table, target_level).await?;
                }

                CompactionResult::Failed {
                    error_value,
                    original_level,
                } => {
                    println!(
                        "Compaction failed for level: {} with error: {:?}",
                        original_level,
                        error_value
                    );
                    if let Some(entry) = self.pending_compactions.get_mut(&original_level) {
                        *entry -= 1;
                        if *entry == 0 {
                            self.pending_compactions.remove(&original_level);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn check_for_compactions_old(&mut self) -> Result<(), LsmError> {
        // Process all available compaction results
        while let Some(pending) = self.wall_e.check_results().await {
            match pending {
                CompactionResult::Completed {
                    compacted_table,
                    original_level,
                    items_processed: _,
                } => {
                    if let Some(entry) = self.pending_compactions.get_mut(&original_level) {
                        *entry -= 1;
                        if *entry == 0 {
                            self.pending_compactions.remove(&original_level);
                        }
                    }

                    let target_level = original_level + 1;
                    println!("Inserting from check_for_compactions");
                    self.insert_new_table(compacted_table, target_level).await?;
                }

                CompactionResult::Failed {
                    error_value,
                    original_level,
                } => {
                    println!(
                        "Compaction failed for level: {} with error: {:?}",
                        original_level,
                        error_value
                    );
                    if let Some(entry) = self.pending_compactions.get_mut(&original_level) {
                        *entry -= 1;
                        if *entry == 0 {
                            self.pending_compactions.remove(&original_level);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn get(&mut self, key: String) -> Result<Arc<KeyValue>, LsmError> {
        self.check_for_compactions().await?;
        if let Some(kv) = self.primary.get(&key) {
            return Ok(kv.into());
        }
        for level in self.levels.iter() {
            for sstable in level.inner.iter() {
                match sstable.get(key.clone()) {
                    Ok(kv) => return Ok(kv),
                    Err(SSTableError::KeyNotfound) => continue,
                    Err(e) => return Err(LsmError::SSTable(e)),
                }
            }
        }

        Err(LsmError::KeyNotFound)
    }

    pub async fn put(&mut self, key: String, value: String) -> Result<(), LsmError> {
        self.check_for_compactions().await?;
        self.primary.put(key, value);
        if self.primary.at_capacity() {
            println!("Inserting new table");
            let sstable = self.flash().await.expect("Flushing thread panicked");
            self.insert_new_table(sstable, 0).await?;
        }
        Ok(())
    }

    fn range() {
        todo!();
    }
}

#[cfg(test)]
mod lsm_database_tests {
    use super::*;
    use memtable::MemTableOperations;
    use tempfile::tempdir;

    fn create_test_db() -> (LsmDatabase, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap().to_string();
        let mut db = LsmDatabase::new(path, None);
        db.primary = MemTableBuilder::default().max_entries(10).build();
        (db, temp_dir)
    }

    fn create_kv(prefix: &str, idx: usize) -> (String, String) {
        let key = format!("{}-key-{}", prefix, idx);
        let value = format!("{}-value-{}", prefix, idx);
        (key, value)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_basic_put_get() {
        let (mut db, _temp_dir) = create_test_db();
        let (key, value) = create_kv("test", 1);
        db.put(key.clone(), value.clone()).await.unwrap();
        let result = db.get(key.clone()).await.unwrap();
        assert_eq!(result.key, key);
        assert_eq!(result.value, value);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_memtable_flush() {
        let (mut db, _temp_dir) = create_test_db();
        for i in 0..1000 {
            let (key, value) = create_kv("flush", i);
            db.put(key, value).await.unwrap();
        }

        assert!(!db.levels[0].inner.is_empty() || !db.levels[1].inner.is_empty());
        // SSTable should be in level 0 or 1
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_simple_compaction() {
        let (mut db, _temp_dir) = create_test_db();

        for i in 0..21 {
            let (key, value) = create_kv("compaction", i);
            db.put(key, value).await.unwrap();
        }

        println!("starting compaction");
        db.check_for_compactions().await.unwrap();
        println!("{:?}", db);

        for i in 0..20 {
            let (key, value) = create_kv("compaction", i);
            let result = db.get(key.clone()).await;
        println!("{:?}", db);
            assert!(result.is_ok(), "Failed to retrieve key: {}", key);
            assert_eq!(result.unwrap().value, value);
        }
        assert!(db.levels.len() > 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_level_compaction() {
        let (mut db, _temp_dir) = create_test_db();
        for i in 0..100 {
            let (key, value) = create_kv("multilevel", i);
            db.put(key, value).await.unwrap();
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
            let result = db.get(key.clone()).await;
            assert!(result.is_ok(), "Failed to retrieve key: {}", key);
            assert_eq!(result.unwrap().value, value);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_update_existing_keys() {
        let (mut db, _temp_dir) = create_test_db();

        // Insert keys
        for i in 0..3 {
            let (key, value) = create_kv("update", i);
            db.put(key, value).await.unwrap();
        }

        // Update the same keys with new values
        for i in 0..3 {
            let (key, _) = create_kv("update", i);
            let new_value = format!("updated-value-{}", i);
            db.put(key, new_value).await.unwrap();
        }

        // Verify that we have the updated values
        for i in 0..3 {
            let (key, _) = create_kv("update", i);
            let expected_value = format!("updated-value-{}", i);
            let result = db.get(key.clone()).await.unwrap();
            assert_eq!(result.value, expected_value);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_interleaved_put_get_with_compaction() {
        let (mut db, _temp_dir) = create_test_db();

        // Phase 1: Insert some keys
        for i in 0..10 {
            let (key, value) = create_kv("interleaved", i);
            db.put(key, value).await.unwrap();
        }

        // Verify all keys from phase 1
        for i in 0..10 {
            let (key, value) = create_kv("interleaved", i);
            let result = db.get(key).await.unwrap();
            assert_eq!(result.value, value);
        }

        // Phase 2: Insert more keys to trigger compaction
        for i in 10..30 {
            let (key, value) = create_kv("interleaved", i);
            db.put(key, value).await.unwrap();
        }

        // Verify all keys from phase 1 and 2
        for i in 0..30 {
            let (key, value) = create_kv("interleaved", i);
            let result = db.get(key).await.unwrap();
            assert_eq!(result.value, value);
        }

        // Phase 3: Insert even more keys for multi-level compaction
        for i in 30..60 {
            let (key, value) = create_kv("interleaved", i);
            db.put(key, value).await.unwrap();
        }

        // Verify all keys are still accessible
        for i in 0..60 {
            let (key, value) = create_kv("interleaved", i);
            let result = db.get(key).await.unwrap();
            assert_eq!(result.value, value);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_lookup_nonexistent_key() {
        let (mut db, _temp_dir) = create_test_db();

        // Insert some keys
        for i in 0..20 {
            let (key, value) = create_kv("exists", i);
            db.put(key, value).await.unwrap();
        }

        // Look up a key that doesn't exist
        let nonexistent_key = "nonexistent-key".to_string();
        let result = db.get(nonexistent_key).await;
        assert!(result.is_err());

        if let Err(err) = result {
            match err {
                LsmError::KeyNotFound => { /* expected error */ }
                _ => panic!("Expected KeyNotFound error, got: {:?}", err),
            }
        }
    }
}
