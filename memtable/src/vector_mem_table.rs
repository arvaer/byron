use key_value::KeyValue;

use crate::MemTableOperations;

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

    fn get(&self, key: &str) -> Option<&String> {
        self.data
            .iter()
            .find(|kv| kv.key == key)
            .map(|kv| &kv.value)
    }

    fn capacity(&self) -> usize {
        self.data.len()
    }

    fn flush(&self) -> Result<(), crate::error::MemTableError> {
        todo!()
    }

}
