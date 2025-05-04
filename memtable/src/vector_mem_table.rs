use key_value::KeyValue;
use sstable::{
    builder::{SSTableBuilder, SSTableFeatures},
    SSTable,
};
use std::collections::BTreeMap;
use std::{path::PathBuf, sync::Arc};

use crate::{MemTableOperations, RangeResult};

#[derive(Debug, Default)]
pub struct VectorMemTable {
    data: Vec<KeyValue>,
    max_entries: usize,
}

impl VectorMemTable {
    pub fn new(max_entries: usize) -> Self {
        Self {
            data: Vec::with_capacity(max_entries),
            max_entries,
        }
    }
}

impl MemTableOperations for VectorMemTable {
    fn put(&mut self, key: String, value: String) {
        self.data.push(KeyValue { key, value })
    }

    fn get(&self, key: &str) -> Option<Box<KeyValue>> {
        self.data
            .iter()
            .find(|kv| kv.key == key)
            .map(|kv| Box::new(kv.clone()))
    }

    fn range(&self, from_m: &str, to_n: &str) -> (Vec<Box<KeyValue>>, RangeResult) {
        let mut deduped = BTreeMap::new();
        for kv in &self.data {
            deduped.insert(kv.key.clone(), kv.clone());
        }

        let mut entries = Vec::new();
        let mut saw_to = false;

        // scan from the first key ≥ from_m
        for (key, kv) in deduped.range(from_m.to_owned()..) {
            if key.as_str() <= to_n {
                entries.push(Box::new(kv.clone()));
                if key == to_n {
                    saw_to = true;
                }
            } else {
                // first key > to_n
                return (entries, RangeResult::FirstKeyFound);
            }
        }

        if entries.is_empty() {
            (entries, RangeResult::KeyNotFound)
        } else if saw_to {
            (entries, RangeResult::FullSetFound)
        } else {
            // we found some entries but never hit to_n,
            // and there was no key > to_n to stop us
            (entries, RangeResult::FirstKeyFound)
        }
    }

    fn at_capacity(&self) -> bool {
        self.data.len() >= self.max_entries
    }

    fn current_length(&self) -> usize {
        self.data.len()
    }

    fn flush(
        &mut self,
        path: PathBuf,
        table_params: SSTableFeatures,
    ) -> Result<Arc<SSTable>, crate::error::MemTableError> {
        let mut builder = SSTableBuilder::new(table_params, &path)?;

        let mut deduped: BTreeMap<String, KeyValue> = BTreeMap::new();
        for kv in &self.data {
            deduped.insert(kv.key.clone(), kv.clone());
        }

        for (_key, kv) in deduped {
            builder.add_from_kv(kv)?;
        }
        let table = builder.build()?;
        Ok(table)
    }
}
