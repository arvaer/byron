use key_value::KeyValue;
use memtable::{mem_table_builder::MemTableBuilder, MemTable, MemTableOperations};
use rayon::prelude::*;
use sstable::{builder::SSTableFeatures, error::SSTableError, SSTable};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, RwLock};
use tokio::task;
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
    // Vector of memtables, newest first (at index 0)
    pub memtables: Arc<Mutex<Vec<(Uuid, Arc<MemTable>)>>>,
    pub levels: Arc<RwLock<Vec<Level>>>,
    pub parent_directory: PathBuf,
    pub capacity_expansion_factor: f64,
    pub base_fpr: f64,
    pub max_memtables: usize, // Limit total memtables to control memory usage
}

impl LsmDatabase {
    pub fn new(data_dir: impl Into<PathBuf>, expand: Option<f64>) -> Self {
        let first = Level {
            inner: Vec::new(),
            depth: 0,
            width: 2,
            total_entries: 0,
        };


        let initial_memtable = Arc::new(MemTableBuilder::default().max_entries(1000).build());
        let initial_id = Uuid::new_v4();

        Self {
            memtables: Arc::new(Mutex::new(vec![(initial_id, initial_memtable)])),
            levels: Arc::new(RwLock::new(vec![first])),
            parent_directory: data_dir.into(),
            capacity_expansion_factor: expand.unwrap_or(1.618),
            base_fpr: 0.005,
            max_memtables: 10,
        }
    }

    // Flash a specific memtable to SSTable
    pub async fn flash_memtable(
        parent_dir: PathBuf,
        memtable: Arc<MemTable>,
    ) -> Result<Arc<SSTable>, LsmError> {

        let features = SSTableFeatures {
            item_count: memtable.current_length(),
            fpr: 0.016,
        };

        let sstable = task::spawn_blocking(move || {
            let path = parent_dir.join(format!("sstable-id-{}", Uuid::new_v4()));
            memtable.flush(path, features).expect("flush failed")
        })
        .await
        .expect("flush task panic");

        Ok(sstable)
    }

    pub async fn get(&self, key: String) -> Result<Arc<KeyValue>, LsmError> {
        let memtables = self.memtables.lock().await;

        for (_, memtable) in memtables.iter() {
            if let Some(kv) = memtable.get(&key) {
                if kv.value == "d34db33f" {
                    return Err(LsmError::KeyNotFound);
                }
                return Ok(kv.into());
            }
        }
        drop(memtables);

        let levels = self.levels.read().await;

        let result = levels
            .par_iter()
            .flat_map(|lvl| lvl.inner.par_iter())
            .find_map_any(|sst| match sst.get(key.clone()) {
                Ok(kv) if kv.value != "d34db33f" => Some(Ok(kv)),
                Ok(_) => Some(Err(LsmError::KeyNotFound)),
                Err(SSTableError::KeyNotfound) => None,
                Err(e) => Some(Err(LsmError::SSTable(e))),
            });

        result.unwrap_or(Err(LsmError::KeyNotFound))
    }

    pub async fn put(&self, key: String, value: String) -> Result<(), LsmError> {
        let mut memtables = self.memtables.lock().await;
        let (active_id, active_memtable) = &memtables[0];

        active_memtable.insert(key, value);

        if active_memtable.at_capacity() {

            let full_table = active_memtable.clone();
            let full_id = *active_id;

            let new_id = Uuid::new_v4();
            let new_table = Arc::new(MemTableBuilder::default().max_entries(1000).build());

            memtables.insert(0, (new_id, new_table));
            drop(memtables);

            let memtables_ref = Arc::clone(&self.memtables);
            let parent_dir = self.parent_directory.clone();

            let sstable = match LsmDatabase::flash_memtable(parent_dir, full_table).await {
                Ok(table) => {
                    table
                }
                Err(e) => {
                    return Err(e);
                }
            };

            let compaction_start = std::time::Instant::now();

            match self.insert_new_table(sstable, 0).await {
                Ok(_) => println!("Compaction completed in {:?}", compaction_start.elapsed()),
                Err(e) => {
                    log::error!("Compaction error: {:?}", e);
                    return Err(e);
                }
            }

            if let Ok(mut memtables) = memtables_ref.try_lock() {
                memtables.retain(|(id, _)| id != &full_id);

            } else {
                log::error!("Warning: Couldn't get lock to remove memtable {}", full_id);
            }

        };
        Ok(())
    }

    pub async fn delete(&self, key: String) -> Result<(), LsmError> {
        self.put(key, "d34db33f".into()).await
    }

    pub async fn range(
        &self,
        from_m: String,
        to_n: String,
    ) -> Result<Vec<Box<KeyValue>>, LsmError> {
        let mut results = Vec::new();
        let mut found_first_key = false;
        let mut start_key = from_m.clone();

        let memtables = &self.memtables.lock().await;

        for (_, memtable) in memtables.iter() {
            let (mut mem_results, flag) = memtable.range(&from_m, &to_n);

            match flag {
                memtable::RangeResult::FullSetFound => {
                    // If any memtable has the full set, we're done
                    return Ok(mem_results);
                }
                memtable::RangeResult::FirstKeyFound => {
                    // Add these results and update our tracking
                    results.append(&mut mem_results);
                    found_first_key = true;
                    // Update start_key to last key found
                    if let Some(last) = results.last() {
                        start_key = last.key.clone();
                    }
                }
                memtable::RangeResult::KeyNotFound => {
                    // This memtable didn't have what we needed, continue to next
                    continue;
                }
            }
        }

        // After checking all memtables, proceed based on what we found
        if found_first_key {
            // We found the first key in at least one memtable
            // Proceed with SSTable search from the start_key
            let levels_guard = self.levels.read().await;

            'outer: for level in levels_guard.iter() {
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
        } else {
            // We didn't find the first key in any memtable
            // Proceed with full SSTable search
            let levels_guard = self.levels.read().await;

            'outer: for level in levels_guard.iter() {
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

        if results.is_empty() {
            Err(LsmError::KeyNotFound)
        } else {
            Ok(results)
        }
    }
}

impl Clone for LsmDatabase {
    fn clone(&self) -> Self {
        Self {
            memtables: Arc::clone(&self.memtables),
            levels: Arc::clone(&self.levels),
            parent_directory: self.parent_directory.clone(),
            capacity_expansion_factor: self.capacity_expansion_factor,
            base_fpr: self.base_fpr,
            max_memtables: self.max_memtables,
        }
    }
}
