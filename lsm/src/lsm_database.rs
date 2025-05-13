use key_value::KeyValue;
use memtable::{mem_table_builder::MemTableBuilder, MemTable, MemTableOperations};
use rayon::prelude::*;
use sstable::{
    builder::{SSTableBuilder, SSTableFeatures},
    error::SSTableError,
    SSTable,
};
use std::{mem, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;
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

pub struct LsmDatabase {
    pub primary: Arc<RwLock<MemTable>>,
    pub levels: Arc<RwLock<Vec<Level>>>,
    pub parent_directory: PathBuf,
    pub capacity_expansion_factor: f64,
    pub base_fpr: f64,
}

impl LsmDatabase {
    pub fn new(data_dir: impl Into<PathBuf>, expand: Option<f64>) -> Self {
        let first = Level {
            inner: Vec::new(),
            depth: 0,
            width: 2,
            total_entries: 0,
        };
        Self {
            primary: Arc::new(RwLock::new(
                MemTableBuilder::default().max_entries(1000).build(),
            )),
            levels: Arc::new(RwLock::new(vec![first])),
            parent_directory: data_dir.into(),
            capacity_expansion_factor: expand.unwrap_or(1.618),
            base_fpr: 0.005,
        }
    }

    fn flash(&self) -> task::JoinHandle<Arc<SSTable>> {
        let primary = self.primary.clone();
        let parent_directory = self.parent_directory.clone();
        let features = {
            let guard = primary.try_read().expect("primary lock poisoned");
            SSTableFeatures {
                item_count: guard.current_length(),
                fpr: 0.016, // or calculate dynamically
            }
        };

        task::spawn_blocking(move || {
            let mut guard = primary.write();
            let mut old = mem::replace(
                &mut *guard,
                MemTableBuilder::default().max_entries(1000).build(),
            );
            drop(guard);

            let path = parent_directory.join(format!("sstable-id-{}", Uuid::new_v4()));
            let table = old.flush(path, features).expect("flush failed");
            return table;
        })
    }

    fn get_sync(&self, key: String) -> Result<Arc<KeyValue>, LsmError> {
        // 1) try memtable
        {
            let guard = self.primary.try_read().expect("poisoned");
            if let Some(kv) = guard.get(&key) {
                if kv.value == "d34db33f" {
                    return Err(LsmError::KeyNotFound);
                }
                return Ok(kv.into());
            }
        }

        let levels = self.levels.try_read().expect("poisoned");
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

    pub async fn get(&self, key: String) -> Result<Arc<KeyValue>, LsmError> {
        task::spawn_blocking({
            let this = self.clone();
            move || this.get_sync(key)
        })
        .await
        .unwrap()
    }

    pub async fn put(&self, key: String, value: String) -> Result<(), LsmError> {
        {
            let mut guard = self.primary.write().await;
            guard.put(key.clone(), value);
            if !guard.at_capacity() {
                return Ok(());
            }
        }

        let table = self.flash().await.expect("flush panicked");

        let mut levels = self.levels.write().await;
        levels[0].inner.push(table);
        Ok(())
    }

    pub async fn delete(&self, key: String) -> Result<(), LsmError> {
        self.put(key, "d34db33f".into()).await
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

// You may need to impl Clone for LsmDatabase manually:
impl Clone for LsmDatabase {
    fn clone(&self) -> Self {
        Self {
            primary: Arc::clone(&self.primary),
            levels: Arc::clone(&self.levels),
            parent_directory: self.parent_directory.clone(),
            capacity_expansion_factor: self.capacity_expansion_factor,
            base_fpr: self.base_fpr,
        }
    }
}
