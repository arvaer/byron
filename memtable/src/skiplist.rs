use crate::{MemTableOperations, RangeResult};
use crossbeam_skiplist::SkipMap;
use key_value::KeyValue;
use sstable::{
    builder::SSTableFeatures, streamed_builder::StreamedSSTableBuilder, SSTable
};
use std::{path::PathBuf, sync::Arc};

#[derive(Debug)]
pub struct CrossBeam {
    inner: SkipMap<String, String>,
    pub max_entries: usize,
}

impl CrossBeam {
    pub fn new(max_entries: usize) -> Self {
        CrossBeam {
            inner: SkipMap::new(),
            max_entries,
        }
    }
}

impl MemTableOperations for CrossBeam {
    fn current_length(&self) -> usize {
        self.inner.len()
    }

    fn at_capacity(&self) -> bool {
        self.current_length() >= self.max_entries
    }
    fn put(&mut self, key: String, value: String) {
        self.inner.insert(key, value);
    }

    fn insert(&self, key: String, value: String) {
        self.inner.insert(key, value);
    }

    fn get(&self, key: &str) -> Option<Box<KeyValue>> {
        self.inner.get(key).map(|entry| {
            Box::new(KeyValue {
                key: entry.key().clone(),
                value: entry.value().clone(),
            })
        })
    }

    fn range(&self, from: &str, to: &str) -> (Vec<Box<KeyValue>>, RangeResult) {
        let mut results = Vec::new();
        let mut saw_to = false;

        for entry in self.inner.range(from.to_string()..) {
            let k = entry.key();
            if k.as_str() <= to {
                results.push(Box::new(KeyValue {
                    key: k.clone(),
                    value: entry.value().clone(),
                }));
                if k == to {
                    saw_to = true;
                }
            } else {
                return (results, RangeResult::FirstKeyFound);
            }
        }

        if results.is_empty() {
            (results, RangeResult::KeyNotFound)
        } else if saw_to {
            (results, RangeResult::FullSetFound)
        } else {
            (results, RangeResult::FirstKeyFound)
        }
    }

    fn flush(
        &self,
        path: PathBuf,
        table_params: SSTableFeatures,
    ) -> Result<Arc<SSTable>, crate::error::MemTableError> {
        let mut builder = StreamedSSTableBuilder::new(table_params, true, &path)?;

        for entry in self.inner.iter() {
            let kv = KeyValue {
                key: entry.key().clone(),
                value: entry.value().clone(),
            };
            builder.add_from_kv(kv)?;
        }

        let table = builder.finalize()?;
        Ok(table)
    }

    fn max_entries(&self) -> usize {
        self.max_entries
    }

}
