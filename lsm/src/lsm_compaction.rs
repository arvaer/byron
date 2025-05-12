use key_value::KeyValue;
use sstable::builder::SSTableFeatures;
use sstable::{streamed_builder::StreamedSSTableBuilder, SSTable};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use uuid::Uuid;

use crate::{
    error::LsmError,
    lsm_database::{Level, LsmDatabase},
};

struct HeapItem {
    key_value: KeyValue,
    sstable_idx: usize,
}

impl Eq for HeapItem {}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key_value == other.key_value
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other.key_value.cmp(&self.key_value)
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub trait Monkey {
    fn extend(&mut self, from: usize) -> Result<(), LsmError>;
    fn insert_new_table(
        &mut self,
        incoming_table: Arc<SSTable>,
        level_number: usize,
    ) -> Result<(), LsmError>;
}

impl Monkey for LsmDatabase {
    fn extend(&mut self, from: usize) -> Result<(), LsmError> {
        log::info!("Extending from level {}", from);
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
    fn insert_new_table(
        &mut self,
        incoming_table: Arc<SSTable>,
        level_number: usize,
    ) -> Result<(), LsmError> {
        log::info!(
            "Step 1: Inserting table with {} entries into level {}",
            incoming_table.actual_item_count,
            level_number
        );

        let mut final_level_flag = false;
        if level_number >= self.levels.len() {
            log::info!(
                "Step 2: Level {} does not exist. Extending database...",
                level_number
            );
            final_level_flag = true;
            self.extend(level_number)?;

            log::info!("Step 2.1: New level layout:");
            for (i, level) in self.levels.iter().enumerate() {
                log::info!(
                    "  Level {}: {} tables, width {}, depth {}",
                    i,
                    level.inner.len(),
                    level.width,
                    level.depth
                );
            }
        }

        log::info!("Step 3: Adding table to level {}", level_number);
        {
            let level = &mut self.levels[level_number];
            level.inner.push(incoming_table.clone());
            level.total_entries += incoming_table.actual_item_count;
            log::info!(
                "Step 3.1: Level {} now has {} tables with {} total entries",
                level_number,
                level.inner.len(),
                level.total_entries
            );
        }

        let needs_compaction = {
            let level = &self.levels[level_number];
            let will_compact = level.inner.len() >= level.width;
            log::info!("Step 4: Checking compaction for level {}: {} tables (width {}), compaction needed: {}",
                 level_number, level.inner.len(), level.width, will_compact);
            will_compact
        };

        if needs_compaction {
            log::info!("Step 5: Starting compaction for level {}", level_number);
            let file_name = self
                .parent_directory
                .join(format!("sstable-id-{}", Uuid::new_v4()));
            let fpr = self.levels[level_number].width as f64 * self.base_fpr;
            let total_entries = self.levels[level_number].total_entries;
            log::info!(
                "Step 5.1: Creating new table with fpr {} and {} entries",
                fpr,
                total_entries
            );

            let features = SSTableFeatures {
                fpr,
                item_count: total_entries,
            };
            let mut min_heap = BinaryHeap::new();
            let mut iterators: Vec<_> = self.levels[level_number]
                .inner
                .iter()
                .map(|table| table.iter())
                .collect();

            for (sstable_idx, iter) in iterators.iter_mut().enumerate() {
                if let Some(kv_result) = iter.next() {
                    let key_value = kv_result?;
                    min_heap.push(HeapItem {
                        key_value,
                        sstable_idx,
                    });
                }
            }
            log::info!(
                "Step 5.2: Initialized min heap with {} items",
                min_heap.len()
            );

            let mut new_table =
                StreamedSSTableBuilder::new(features, !final_level_flag, &file_name)?;
            let mut items_processed = 0;
            while let Some(HeapItem {
                key_value,
                sstable_idx,
            }) = min_heap.pop()
            {
                let _ = new_table.add_from_kv(key_value);
                items_processed += 1;
                if let Some(next_kv_result) = iterators[sstable_idx].next() {
                    let next_kv = next_kv_result?;
                    if next_kv.value == "d34db33f" {
                        continue;
                    }
                    min_heap.push(HeapItem {
                        key_value: next_kv,
                        sstable_idx,
                    });
                }
            }
            log::info!(
                "Step 5.3: Processed {} items during compaction",
                items_processed
            );

            let compacted_table = new_table.finalize()?;
            log::info!(
                "Step 5.4: Finalized new table with {} entries",
                compacted_table.actual_item_count
            );

            for table in self.levels[level_number].inner.iter() {
                table.delete()?;
            }
            self.levels[level_number].inner.clear();
            log::info!("Step 5.5: Cleared level {}", level_number);

            log::info!(
                "Step 5.6: Recursively inserting compacted table into level {}",
                level_number + 1
            );
            self.insert_new_table(compacted_table, level_number + 1)?;
        }

        log::info!("Step 6: Insertion complete for level {}", level_number);
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

    #[test]
    fn test_extend() {
        let mut db = create_test_db();

        // Test extending from level 0
        db.extend(0).unwrap();
        assert_eq!(db.levels.len(), 2);
        assert_eq!(db.levels[1].width, 3); // 2 * 1.5 = 3
        assert_eq!(db.levels[1].depth, 1);
        assert_eq!(db.levels[1].inner.len(), 0);

        // Test extending from level 1
        db.extend(1).unwrap();
        assert_eq!(db.levels.len(), 3);
        assert_eq!(db.levels[2].width, 4); // 3 * 1.5 = 4.5, rounded to 4
        assert_eq!(db.levels[2].depth, 2);
    }

    #[test]
    fn test_insert_without_compaction() {
        let mut db = create_test_db();
        let temp_dir = tempdir().unwrap();

        // Create a test SSTable
        let sstable = create_test_sstable(1, 10, &temp_dir.path().to_path_buf());

        // Insert into level 0
        db.insert_new_table(sstable.clone(), 0).unwrap();

        // Verify it was inserted
        assert_eq!(db.levels[0].inner.len(), 1);
        assert_eq!(db.levels[0].total_entries, 10);
    }

    #[test]
    fn test_insert_with_compaction() {
        let mut db = create_test_db();
        let temp_dir = tempdir().unwrap();

        // Level 0 has width of 2, so adding 2 tables should trigger compaction
        let sstable1 = create_test_sstable(1, 10, &temp_dir.path().to_path_buf());
        let sstable2 = create_test_sstable(2, 15, &temp_dir.path().to_path_buf());

        // Insert first table (shouldn't trigger compaction)
        db.insert_new_table(sstable1, 0).unwrap();
        assert_eq!(db.levels[0].inner.len(), 1);
        assert_eq!(db.levels.len(), 1);

        // Insert second table (should trigger compaction and create level 1)
        db.insert_new_table(sstable2, 0).unwrap();

        // Level 0 should be cleared after compaction
        assert_eq!(db.levels[0].inner.len(), 0);

        // Level 1 should exist with 1 table containing all entries
        assert_eq!(db.levels.len(), 2);
        assert_eq!(db.levels[1].inner.len(), 1);
        assert_eq!(db.levels[1].total_entries, 25); // 10 + 15
    }

    #[test]
    fn test_multi_level_compaction() {
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
        db.insert_new_table(tables[0].clone(), 0).unwrap();
        db.insert_new_table(tables[1].clone(), 0).unwrap();

        // Level 0 should be empty, level 1 should have 1 compacted table
        assert_eq!(db.levels[0].inner.len(), 0);
        assert_eq!(db.levels[1].inner.len(), 1);

        // Insert 2 more tables to level 0, triggering another compaction to level 1
        db.insert_new_table(tables[2].clone(), 0).unwrap();
        db.insert_new_table(tables[3].clone(), 0).unwrap();

        // Level 1 should now have 2 tables
        assert_eq!(db.levels[1].inner.len(), 2);

        // Insert 1 more table directly to level 1, triggering compaction to level 2
        db.insert_new_table(tables[4].clone(), 1).unwrap();

        // Level 1 should be empty, level 2 should have 1 compacted table
        assert_eq!(db.levels[1].inner.len(), 0);
        assert_eq!(db.levels[2].inner.len(), 1);
        assert_eq!(db.levels[2].total_entries, 50); // 5 tables * 10 entries
    }
}
