use key_value::KeyValue;
use sstable::builder::SSTableFeatures;
use sstable::{streamed_builder::StreamedSSTableBuilder, SSTable};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use uuid::Uuid;

use crate::wall_e::CompactionTask;
use crate::level::Level;
use crate::{
    error::LsmError,
    lsm_database::LsmDatabase,
};

impl LsmDatabase {
    async fn extend(&mut self, from: usize) -> Result<(), LsmError> {
        println!("Extending from level {}", from);
        if self.levels.is_empty() {
            return Err(LsmError::Other("Cannot extend empty database".to_string()));
        }

        let reference_level_idx = self.levels.len() - 1;

        let lrl = &self.levels[reference_level_idx];
        let new_width = (lrl.width as f64 * self.capacity_expansion_factor) as usize;

        let new_level = Level {
            inner: Vec::new(),
            depth: from + 1,
            width: new_width,
            total_entries: 0,
        };

        self.levels.push(new_level);
        Ok(())
    }

    pub async fn insert_new_table(
        &mut self,
        incoming_table: Arc<SSTable>,
        level_number: usize,
    ) -> Result<(), LsmError> {
        println!(
            "Step 1: Inserting table with {} entries into level {}",
            incoming_table.actual_item_count, level_number
        );

        let mut final_level_flag = false;
        if level_number >= self.levels.len() {
            println!(
                "Step 2: Level {} does not exist. Extending database...",
                level_number
            );
            final_level_flag = true;
            self.extend(level_number).await?;

            println!("Step 2.1: New level layout:");
            for (i, level) in self.levels.iter().enumerate() {
                println!(
                    "  Level {}: {} tables, width {}, depth {}",
                    i,
                    level.inner.len(),
                    level.width,
                    level.depth
                );
            }
        }

        {
            let level = &mut self.levels[level_number];
            println!("Step 3: Adding table to level {}", level_number);
            level.inner.push(incoming_table.clone());
            level.total_entries += incoming_table.actual_item_count;
            println!(
                "Step 3.1: Level {} now has {} tables with {} total entries",
                level_number,
                level.inner.len(),
                level.total_entries
            );
        }

        let needs_compaction: bool =
            { self.levels[level_number].inner.len() >= self.levels[level_number].width };

        if needs_compaction {
            println!("Step 4: Level {} needs compaction", level_number);
            let compaction_task = CompactionTask::CompactLevel {
                level: self.levels[level_number].clone(),
                base_fpr: self.base_fpr,
                parent_directory: self.parent_directory.clone(),
                bloom_enabled: !final_level_flag,
            };

            let _ = self.wall_e.send_task(compaction_task).await;

            *self.pending_compactions.entry(level_number).or_insert(0) += 1;

            println!("Step 4.1: Level {} cleared for compaction", level_number);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn create_test_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn create_test_sstable(id: u32, item_count: usize, dir: &PathBuf) -> Arc<SSTable> {
        let file_name = dir.join(format!("test-sstable-{}", id));
        let features = SSTableFeatures {
            fpr: 0.01,
            item_count,
        };

        let mut builder = StreamedSSTableBuilder::new(features, true, &file_name).unwrap();

        // Add some test key-values
        for i in 0..item_count {
            let key = format!("key-{}-{}", id, i);
            let value = format!("value-{}-{}", id, i);
            let _ = builder.add_from_kv(KeyValue { key, value });
        }

        builder.finalize().unwrap()
    }

    // Helper to create a test database
    fn create_test_db() -> LsmDatabase {
        let mut db = LsmDatabase::new(String::from("./test"), None);
        if db.levels.is_empty() {
            db.levels.push(Level {
                inner: Vec::new(),
                depth: 0,
                width: 2,
                total_entries: 0,
            });
        }
        return db;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_extend() {
        let mut db = create_test_db();

        // Test extending from level 0
        db.extend(0).await.unwrap();
        assert_eq!(db.levels.len(), 2);
        assert_eq!(db.levels[1].width, 3); // 2 * 1.5 = 3
        assert_eq!(db.levels[1].depth, 1);
        assert_eq!(db.levels[1].inner.len(), 0);

        // Test extending from level 1
        db.extend(1).await.unwrap();
        assert_eq!(db.levels.len(), 3);
        assert_eq!(db.levels[2].width, 4); // 3 * 1.5 = 4.5, rounded to 4
        assert_eq!(db.levels[2].depth, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_insert_without_compaction() {
        let mut db = create_test_db();
        let temp_dir = tempdir().unwrap();

        // Create a test SSTable
        let sstable = create_test_sstable(1, 10, &temp_dir.path().to_path_buf());

        // Insert into level 0
        db.insert_new_table(sstable.clone(), 0).await.unwrap();

        // Verify it was inserted
        assert_eq!(db.levels[0].inner.len(), 1);
        assert_eq!(db.levels[0].total_entries, 10);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_insert_with_compaction() {
        let mut db = create_test_db();
        let temp_dir = tempdir().unwrap();

        // Level 0 has width of 2, so adding 2 tables should trigger compaction
        let sstable1 = create_test_sstable(1, 10, &temp_dir.path().to_path_buf());
        let sstable2 = create_test_sstable(2, 15, &temp_dir.path().to_path_buf());

        // Insert first table (shouldn't trigger compaction)
        db.insert_new_table(sstable1, 0).await.unwrap();
        assert_eq!(db.levels[0].inner.len(), 1);
        assert_eq!(db.levels.len(), 1);

        // Insert second table (should trigger compaction and create level 1)
        db.insert_new_table(sstable2, 0).await.unwrap();

        // Level 0 should be cleared after compaction
        assert_eq!(db.levels[0].inner.len(), 0);

        // Level 1 should exist with 1 table containing all entries
        assert_eq!(db.levels.len(), 2);
        assert_eq!(db.levels[1].inner.len(), 1);
        assert_eq!(db.levels[1].total_entries, 25); // 10 + 15
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_level_compaction() {
        let mut db = create_test_db();
        let temp_dir = tempdir().unwrap();

        // Set up to trigger multiple levels of compaction
        // Level 0: width 2
        // Level 1: width 3
        // Level 2: width 4

        // Create test tables
        let tables = (0..7)
            .map(|i| create_test_sstable(i, 10, &temp_dir.path().to_path_buf()))
            .collect::<Vec<_>>();

        // Insert 2 tables to level 0, triggering compaction to level 1
        db.insert_new_table(tables[0].clone(), 0).await.unwrap();
        db.insert_new_table(tables[1].clone(), 0).await.unwrap();

        // Level 0 should be empty, level 1 should have 1 compacted table
        assert_eq!(db.levels[0].inner.len(), 0);
        assert_eq!(db.levels[1].inner.len(), 1);

        // Insert 2 more tables to level 0, triggering another compaction to level 1
        db.insert_new_table(tables[2].clone(), 0).await.unwrap();
        db.insert_new_table(tables[3].clone(), 0).await.unwrap();

        // Level 1 should now have 2 tables
        assert_eq!(db.levels[1].inner.len(), 2);

        // Insert 1 more table directly to level 1, triggering compaction to level 2
        db.insert_new_table(tables[4].clone(), 1).await.unwrap();

        // Level 1 should be empty, level 2 should have 1 compacted table
        assert_eq!(db.levels[1].inner.len(), 0);
        assert_eq!(db.levels[2].inner.len(), 1);
        assert_eq!(db.levels[2].total_entries, 50); // 5 tables * 10 entries
    }
}
