use key_value::KeyValue;
use sstable::builder::SSTableFeatures;
use sstable::{ streamed_builder::StreamedSSTableBuilder, SSTable};
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

trait Monkey {
    fn extend(&mut self, from: usize) -> Result<(), LsmError>;
    fn insert_new_table(
        &mut self,
        incoming_table: Arc<SSTable>,
        level_number: usize,
    ) -> Result<(), LsmError>;
    fn compact_level(
        &mut self,
        level: &mut Level,
        final_level_flag: bool,
    ) -> Result<Arc<SSTable>, LsmError>;
}

impl Monkey for LsmDatabase {
    fn extend(&mut self, from: usize) -> Result<(), LsmError> {
        let lrl = &self.levels[from];
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
        let mut final_level_flag = false;
        if level_number > self.levels.len() {
            final_level_flag = true;
            self.extend(level_number)?;
        }
        let level = &mut self.levels[level_number];
        level.inner.push(incoming_table.clone());
        level.total_entries += incoming_table.actual_item_count;
        if level.width > level.inner.len() {
            // compact, and then recurse
            let table = self.compact_level(level, final_level_flag)?;
            self.insert_new_table(table, level_number + 1);
        }
        Ok(())
    }

    fn compact_level(
        &mut self,
        level: &mut Level,
        final_level_flag: bool,
    ) -> Result<Arc<SSTable>, LsmError> {
        let file_name = self
            .parent_directory
            .join(format!("sstable-id-{}", Uuid::new_v4()));
        let fpr = level.width as f64 * self.base_fpr;
        let features = SSTableFeatures {
            fpr,
            item_count: level.total_entries,
        };

        let mut min_heap = BinaryHeap::new();

        let mut iterators: Vec<_> = level.inner.iter().map(|table| table.iter()).collect();

        for (sstable_idx, iter) in iterators.iter_mut().enumerate() {
            if let Some(kv) = iter.next() {
                min_heap.push(HeapItem {
                    key_value: kv?,
                    sstable_idx,
                })
            }
        }
        let mut new_table = StreamedSSTableBuilder::new(features, !final_level_flag, &file_name)?;
        while let Some(HeapItem {
            key_value,
            sstable_idx,
        }) = min_heap.pop()
        {
            new_table.add_from_kv(key_value);

            if let Some(next_kv) = iterators[sstable_idx].next() {
                min_heap.push(HeapItem {
                    key_value: next_kv?,
                    sstable_idx,
                })
            }
        }

        level.inner.clear();
        return Ok(new_table.finalize()?);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
}
