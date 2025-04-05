use key_value::KeyValue;
use sstable::{
    builder::{SSTableBuilder, SSTableFeatures},
    SSTable,
};
use std::{path::PathBuf, sync::Arc};

use crate::MemTableOperations;

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
        if let Some(index) = self.data.iter().position(|item| item.key == key) {
            self.data.remove(index);
        }
        self.data.push(KeyValue { key, value })
    }

    fn get(&self, key: &str) -> Option<Box<KeyValue>> {
        self.data
            .iter()
            .find(|kv| kv.key == key)
            .map(|kv| Box::new(kv.clone()))
    }

    fn at_capacity(&self) -> bool {
        self.data.len() >= self.max_entries
    }

    fn flush(
        &mut self,
        path: PathBuf,
        table_params: SSTableFeatures,
    ) -> Result<Arc<SSTable>, crate::error::MemTableError> {
        let mut builder = SSTableBuilder::new(table_params, &path)?;
        self.data.sort();
        for i in &self.data{
            let _ = builder.add_from_kv(i.clone());
        }
        let builder = builder.build()?;
        Ok(builder)

    }
}
