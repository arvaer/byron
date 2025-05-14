use key_value::KeyValue;
use sstable::builder::SSTableFeatures;
use sstable::{streamed_builder::StreamedSSTableBuilder, SSTable};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::task;
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
const TOTAL_BLOOM_BUDGET: usize = 1_000_000;

impl LsmDatabase {
    /// this implements the monkey‐paper solution:
    ///   argmin(∑ exp(−b_i·ln2))
    ///   =>  ∑ n_i·b_i = M
    ///
    /// The closed‐form is:
    ///   b_i = (M/N) + (∑ n_j·log2(n_j))/N  − log2(n_i)
    pub fn allocate_bloom_bits(level_counts: &[usize], total_bits: usize) -> Vec<f64> {
        // 1. Convert to f64 and sum total entries N
        let n: Vec<f64> = level_counts.iter().map(|&c| c as f64).collect();
        let n_sum: f64 = n.iter().sum();
        assert!(n_sum > 0.0, "Must have at least one entry across levels");

        // 2. compute ∑ n_j·log2(n_j)
        let sum_n_log: f64 = n.iter().map(|&ni| ni * ni.log2()).sum();

        let m_over_n_sum = (total_bits as f64) / n_sum;
        let avg_log = sum_n_log / n_sum;

        // 4. compute b_i = (m/n) + avg_log − log2(n_i)
        n.iter()
            .map(|&ni| {
                // if ni is zero, treat log2(ni) → 0 and clamp
                if ni <= 1.0 {
                    // With 0 or 1 entry, best you can do is allocate at least 1 bit
                    f64::max(m_over_n_sum + avg_log, 1.0)
                } else {
                    let bi = m_over_n_sum + avg_log - ni.log2();
                    // bits-per-entry must be non-negative
                    f64::max(bi, 0.0)
                }
            })
            .collect()
    }

    pub async fn insert_new_table(
        &self,
        mut incoming_table: Arc<SSTable>,
        mut level_number: usize,
    ) -> Result<(), LsmError> {
        // Use a loop instead of recursion for moving tables up levels
        loop {
            println!(
                "Step 1: Inserting table with {} entries into level {}",
                incoming_table.actual_item_count, level_number
            );

            // Step 1: Check if we need to extend levels
            let mut final_level_flag = false;
            {
                let levels = self.levels.read().await;
                if level_number >= levels.len() {
                    drop(levels); // Release read lock before getting write lock
                    println!(
                        "Step 2: Level {} does not exist. Extending database...",
                        level_number
                    );
                    final_level_flag = true;
                    self.extend(level_number).await?;

                    println!("Step 2.1: New level layout:");
                    let levels = self.levels.read().await;
                    for (i, level) in levels.iter().enumerate() {
                        println!(
                            "  Level {}: {} tables, width {}, depth {}",
                            i,
                            level.inner.len(),
                            level.width,
                            level.depth
                        );
                    }
                }
            }

            // Step 2: Add table to the level
            println!("Step 3: Adding table to level {}", level_number);
            {
                let mut levels = self.levels.write().await;
                let level = &mut levels[level_number];
                level.inner.push(incoming_table.clone());
                level.total_entries += incoming_table.actual_item_count;
                println!(
                    "Step 3.1: Level {} now has {} tables with {} total entries",
                    level_number,
                    level.inner.len(),
                    level.total_entries
                );
            }

            // Step 3: Check if compaction is needed
            let needs_compaction = {
                let levels = self.levels.read().await;
                let level = &levels[level_number];

                // Prioritize compaction at level 0 by using a lower threshold if necessary
                // Level 0 compaction is more critical to prevent bottlenecks
                let is_level_0 = level_number == 0;
                let threshold = if is_level_0 {
                    // If we're at level 0, be more aggressive with compaction
                    // Use either the defined width or a dynamic threshold based on total tables
                    std::cmp::min(level.width, 4) // Max 4 tables in level 0 to keep queries fast
                } else {
                    level.width
                };

                let will_compact = level.inner.len() >= threshold;
                println!("Step 4: Checking compaction for level {}: {} tables (threshold {}), compaction needed: {}",
                 level_number, level.inner.len(), threshold, will_compact);
                will_compact
            };

            // If no compaction needed, we're done
            if !needs_compaction {
                println!(
                    "No compaction needed for level {}, we're done",
                    level_number
                );
                return Ok(());
            }

            // Step 4: Perform compaction if needed
            println!("Step 5: Starting compaction for level {}", level_number);

            // Gather all necessary data before spawning the blocking task
            let file_name = self
                .parent_directory
                .join(format!("sstable-id-{}", Uuid::new_v4()));
            let level_counts: Vec<usize>;
            let tables_to_compact: Vec<Arc<SSTable>>;
            let total_entries;

            // Get all data while holding the read lock
            {
                let levels = self.levels.read().await;
                level_counts = levels.iter().map(|lvl| lvl.total_entries).collect();
                tables_to_compact = levels[level_number].inner.clone();
                total_entries = levels[level_number].total_entries;
            }

            // Calculate bloom filter parameters
            let bits_per_entry =
                LsmDatabase::allocate_bloom_bits(&level_counts, TOTAL_BLOOM_BUDGET);
            let fprs: Vec<f64> = bits_per_entry
                .iter()
                .map(|&b| {
                    // Fix for "Invalid false positive rate: 1" error
                    // Clamp between 0.001 and 0.999
                    f64::max(0.001, f64::min(2f64.powf(-b), 0.999))
                })
                .collect();

            let fpr = fprs.get(level_number).cloned().unwrap_or(self.base_fpr);
            println!(
                "Step 5.1: Creating new table with fpr {} and {} entries",
                fpr, total_entries
            );

            let features = SSTableFeatures {
                fpr,
                item_count: total_entries,
            };

            // Use task::spawn_blocking for CPU-intensive compaction
            let compacted_table =
                task::spawn_blocking(move || -> Result<Arc<SSTable>, LsmError> {
                    println!("Inside compaction task for level {}", level_number);

                    // Create iterators for all tables
                    let mut iterators: Vec<_> =
                        tables_to_compact.iter().map(|table| table.iter()).collect();

                    // Initialize min heap
                    let mut min_heap = BinaryHeap::new();
                    for (sstable_idx, iter) in iterators.iter_mut().enumerate() {
                        if let Some(kv_result) = iter.next() {
                            let key_value = match kv_result {
                                Ok(kv) => kv,
                                Err(e) => return Err(LsmError::SSTable(e)),
                            };
                            min_heap.push(HeapItem {
                                key_value,
                                sstable_idx,
                            });
                        }
                    }
                    println!(
                        "Step 5.2: Initialized min heap with {} items",
                        min_heap.len()
                    );

                    // Create streamed builder
                    let mut new_table = match StreamedSSTableBuilder::new(
                        features,
                        !final_level_flag,
                        &file_name,
                    ) {
                        Ok(builder) => builder,
                        Err(e) => return Err(LsmError::SSTable(e)),
                    };

                    // Merge sort from heap
                    let mut items_processed = 0;
                    while let Some(HeapItem {
                        key_value,
                        sstable_idx,
                    }) = min_heap.pop()
                    {
                        let _ = new_table.add_from_kv(key_value);
                        items_processed += 1;
                        if let Some(next_kv_result) = iterators[sstable_idx].next() {
                            let next_kv = match next_kv_result {
                                Ok(kv) => kv,
                                Err(e) => return Err(LsmError::SSTable(e)),
                            };
                            if next_kv.value == "d34db33f" {
                                // deletion sentinel value
                                continue;
                            }
                            min_heap.push(HeapItem {
                                key_value: next_kv,
                                sstable_idx,
                            });
                        }
                    }
                    println!(
                        "Step 5.3: Processed {} items during compaction",
                        items_processed
                    );

                    // Finalize the new table
                    match new_table.finalize() {
                        Ok(table) => {
                            println!("Finalizing compacted SSTable");
                            println!("Finalized compacted SSTable");
                            Ok(table)
                        }
                        Err(e) => Err(LsmError::SSTable(e)),
                    }
                })
                .await.unwrap()?;

            println!(
                "Step 5.4: Finalized new table with {} entries",
                compacted_table.actual_item_count
            );

            // Clear the current level
            {
                let mut levels = self.levels.write().await;
                // Delete the old tables from disk
                for table in &levels[level_number].inner {
                    if let Err(e) = table.delete() {
                        eprintln!("Warning: Failed to delete table: {:?}", e);
                    }
                }
                levels[level_number].inner.clear();
                levels[level_number].total_entries = 0;
                println!("Step 5.5: Cleared level {}", level_number);
            }

            // Instead of recursion, update the loop variables and continue
            println!("Step 5.6: Moving to next level: {}", level_number + 1);
            incoming_table = compacted_table;
            level_number += 1;

            // The loop will now iterate with the new table at the new level
        }
    }

    // Also update the extend method to be async
    pub async fn extend(&self, target_level: usize) -> Result<(), LsmError> {
        println!(
            "Extending levels from {} to {}",
            self.levels.read().await.len(),
            target_level + 1
        );

        let mut levels = self.levels.write().await;
        if levels.is_empty() {
            return Err(LsmError::Other("Cannot extend empty database".to_string()));
        }

        let reference_level_idx = levels.len() - 1;
        let lrl = &levels[reference_level_idx];
        let new_width = (lrl.width as f64 * self.capacity_expansion_factor) as usize;

        while levels.len() <= target_level {
            let new_level = Level {
                inner: Vec::new(),
                depth: levels.len(),
                width: new_width,
                total_entries: 0,
            };

            println!(
                "Adding new level with width {} and depth {}",
                new_width,
                levels.len()
            );
            levels.push(new_level);
        }

        println!("Extended levels, now have {} levels", levels.len());
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
