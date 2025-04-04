use crate::{builder::SSTableFeatures, error::SSTableError, SSTable};
use bloomfilter::Bloom;
use integer_encoding::VarInt;
use key_value::{key_value_pair::DeltaEncodedKV, KeyValue};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

const BLOCK_SIZE: usize = 4096; // 4KB block size
const RESTART_INTERVAL: usize = 16;

#[derive(Debug)]
pub struct StreamedSSTableBuilder {
    pub fence_pointers: Vec<(Arc<str>, usize)>,
    pub last_key: Option<KeyValue>,
    pub file_name: PathBuf,
    pub file_writer: BufWriter<File>,
    pub block: Vec<DeltaEncodedKV>, // Current block being built
    pub block_size: usize,          // Current block size in bytes
    pub block_idx: usize,
    pub page_hash_indices: Vec<HashMap<String, usize>>, // One hash index per block
    pub current_offset: usize,                          // File offset
    pub restart_indices: Vec<Vec<usize>>,               // Restart indices for each block
    pub entry_count: usize,
    pub filter: Bloom<String>,
}

impl StreamedSSTableBuilder {
    pub fn new(
        SSTableFeatures { item_count, fpr }: SSTableFeatures,
        file_name: &Path,
    ) -> Result<Self, SSTableError> {
        if let Some(parent) = file_name.parent() {
            fs::create_dir_all(parent).map_err(SSTableError::FileSystemError)?;
        }
        let file = File::create(file_name).map_err(SSTableError::FileSystemError)?;
        let mut writer = BufWriter::new(file);
        writer
            .write_all(b"SSTB")
            .map_err(SSTableError::FileSystemError)?;
        if fpr <= 0.0 || fpr >= 1.0 {
            return Err(SSTableError::InvalidFalsePositiveRate(fpr));
        }

        let filter = Bloom::new_for_fp_rate(item_count, fpr)
            .map_err(|e| SSTableError::BloomFilterError(e.to_string()))?;

        Ok(Self {
            fence_pointers: Vec::new(),
            last_key: None,
            file_name: file_name.to_path_buf(),
            file_writer: writer,
            block: Vec::new(),
            block_idx: 0,
            block_size: 0,
            page_hash_indices: Vec::new(),
            current_offset: 4, // "SSTB"
            restart_indices: Vec::new(),
            entry_count: 0,
            filter,
        })
    }

    pub fn add_from_kv(&mut self, key: KeyValue) -> Result<(), SSTableError> {
        if key.key.is_empty() {
            return Err(SSTableError::EmptyKey);
        }
        self.filter.set(&key.key);

        let tentative = DeltaEncodedKV::forward(self.last_key.clone(), key.clone());
        let entry_size = tentative.calculate_size();
        if self.block_size + entry_size > BLOCK_SIZE && !self.block.is_empty() {
            let _ = self.seal_current_block();
        }

        if self.block.is_empty() {
            self.restart_indices.push(vec![0]);
            self.page_hash_indices.push(HashMap::new());
            self.fence_pointers
                .push((key.key.clone().into(), self.current_offset));
            self.last_key = None;
        } else if self.block.len() % RESTART_INTERVAL == 0 {
            if let Some(restart_points) = self.restart_indices.last_mut() {
                restart_points.push(self.block_size);
            }
            if let Some(current_hash_index) = self.page_hash_indices.last_mut() {
                current_hash_index.insert(
                    key.key.clone(),
                    self.restart_indices.last().unwrap().len() - 1,
                );
            }
            self.last_key = None;
        }
        // recompute is hacky but idk .
        let dkv = DeltaEncodedKV::forward(self.last_key.clone(), key.clone());
        let entry_size = dkv.calculate_size();
        self.block.push(dkv);
        self.entry_count += 1;
        self.block_size += entry_size;
        self.last_key = Some(key);
        Ok(())
    }

    pub fn seal_current_block(&mut self) -> Result<(), SSTableError> {
        if self.restart_indices.len() <= self.block_idx {
            self.restart_indices.push(Vec::new());
        }

        if self.page_hash_indices.len() <= self.block_idx {
            self.page_hash_indices.push(HashMap::new());
        }
        self.current_offset += self.block_size;
        let block = std::mem::take(&mut self.block);
        self.block_idx += 1;
        self.block_size = 0;
        for kv in block {
            let kv_bytes = kv.to_str();
            let _ = self
                .file_writer
                .write_all(&kv_bytes)
                .map_err(SSTableError::FileSystemError);
        }

        Ok(())
    }

    pub fn finalize(mut self) -> Result<Arc<SSTable>, SSTableError> {
        if !self.block.is_empty() {
            let _ = self.seal_current_block();
        }

        self.file_writer
            .write_all(b"SSTB")
            .map_err(SSTableError::FileSystemError)?;
        self.file_writer
            .flush()
            .map_err(SSTableError::FileSystemError)?;

        Ok(Arc::new(SSTable {
            file_path: self.file_name.clone(),
            fd: None,
            page_hash_indices: self.page_hash_indices.clone(),
            fence_pointers: self.fence_pointers.clone(),
            restart_indices: self.restart_indices.clone(),
            bloom_filter: Arc::new(self.filter),
            actual_item_count: self.entry_count
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use key_value::KeyValue;
    use std::fs;
    use tempfile::tempdir;

    fn create_test_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn test_new_streamed_builder() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let fp = temp_dir.path().join("test.sst");
        let features = SSTableFeatures {
            item_count: 100,
            fpr: 0.01,
        };

        let builder = StreamedSSTableBuilder::new(features, &fp)?;
        assert_eq!(builder.entry_count, 0);
        assert_eq!(builder.block_idx, 0);
        assert!(builder.fence_pointers.is_empty());
        assert!(builder.block.is_empty());
        Ok(())
    }

    #[test]
    fn test_add_single_key() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let features = SSTableFeatures {
            item_count: 100,
            fpr: 0.01,
        };

        let builder = StreamedSSTableBuilder::new(features, &fp)?;
        let kv = create_test_kv("test-key", "test-value");
        builder.add_from_kv(kv)?;

        assert_eq!(builder.entry_count, 1);
        assert_eq!(builder.block.len(), 1);
        Ok(())
    }

    #[test]
    fn test_empty_key() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let fp = temp_dir.path().join("test.sst");

        let features = SSTableFeatures {
            item_count: 100,
            fpr: 0.01,
        };

        let mut builder = StreamedSSTableBuilder::new(features, &fp)?;

        let kv = create_test_kv("", "test-value");
        let result = builder.add_from_kv(kv);

        assert!(matches!(result, Err(SSTableError::EmptyKey)));

        Ok(())
    }

    #[test]
    fn test_streamed_key() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let fp = temp_dir.path().join("test.sst");
        let features = SSTableFeatures {
            item_count: 100,
            fpr: 0.01,
        };

        let mut builder = StreamedSSTableBuilder::new(features, &fp)?;

        for i in 0..100 {
            let key = format!("key-{:05}", i);
            let value = "x".repeat(100);
            builder.add_from_kv(create_test_kv(&key, &value))?;
        }

        assert!(builder.block_idx > 0);
        builder.seal_current_block()?;
        assert!(builder.block.is_empty());

        Ok(())
    }

    #[test]
    fn test_streamed_restart_points() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let fp = temp_dir.path().join("test.sst");
        let features = SSTableFeatures {
            item_count: 100,
            fpr: 0.01,
        };

        let mut builder = StreamedSSTableBuilder::new(features, &fp)?;

        for i in 0..50 {
            let key = format!("key-{:05}", i);
            builder.add_from_kv(create_test_kv(&key, "value"))?;
        }

        builder.seal_current_block()?;

        assert!(!builder.restart_indices.is_empty());

        let restart_points = &builder.restart_indices[0];
        assert!(!restart_points.is_empty());

        // first restart point should be at index 0
        assert_eq!(restart_points[0], 0);

        // should have approximately 50/16 ≈ 3 restart points
        assert!(restart_points.len() >= 3);

        Ok(())
    }

    #[test]
    fn test_streamed_fences() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let fp = temp_dir.path().join("test.sst");
        let features = SSTableFeatures {
            item_count: 100,
            fpr: 0.01,
        };

        let mut builder = StreamedSSTableBuilder::new(features, &fp)?;

        for i in 0..200 {
            let key = format!("key-{:05}", i);
            let value = "x".repeat(50);
            builder.add_from_kv(create_test_kv(&key, &value))?;
        }

        assert!(!builder.fence_pointers.is_empty());
        Ok(())
    }

    #[test]
    fn test_finalize_block() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let fp = temp_dir.path().join("test.sst");
        let features = SSTableFeatures {
            item_count: 100,
            fpr: 0.01,
        };

        let mut builder = StreamedSSTableBuilder::new(features, &fp)?;

        for i in 0..200 {
            let key = format!("key-{:05}", i);
            builder.add_from_kv(create_test_kv(&key, "value"))?;
        }

        let sstable = builder.finalize()?;

        assert!(sstable.file_path.exists());

        let contents = fs::read(&sstable.file_path)?;
        assert!(contents.starts_with(b"SSTB"));
        assert!(contents.ends_with(b"SSTB"));

        Ok(())
    }

    #[test]
    fn test_streamed_dkv() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let fp = temp_dir.path().join("test.sst");
        let features = SSTableFeatures {
            item_count: 100,
            fpr: 0.01,
        };

        let mut builder = StreamedSSTableBuilder::new(features, &fp)?;

        builder.add_from_kv(create_test_kv("user:1000:profile", "value1"))?;
        builder.add_from_kv(create_test_kv("user:1000:settings", "value2"))?;
        builder.add_from_kv(create_test_kv("user:1001:profile", "value3"))?;

        let sstable = builder.finalize()?;
        assert!(sstable.file_path.exists());
        let contents = fs::read(&sstable.file_path)?;
        assert!(contents.starts_with(b"SSTB"));
        assert!(contents.ends_with(b"SSTB"));
        Ok(())
    }

    #[test]
    fn test_eb_bc() -> Result<(), SSTableError> {
        let temp_dir = tempdir().unwrap();
        let fp = temp_dir.path().join("test.sst");
        let features = SSTableFeatures {
            item_count: 100,
            fpr: 0.01,
        };

        let mut builder = StreamedSSTableBuilder::new(features, &fp)?;
        assert_eq!(builder.entry_count, 0);
        assert_eq!(builder.block_idx, 0);

        for i in 0..10 {
            builder.add_from_kv(create_test_kv(&format!("key-{}", i), "value"))?;
        }
        assert_eq!(builder.entry_count, 10);
        assert_eq!(builder.block_idx, 0);
        builder.seal_current_block()?;
        assert_eq!(builder.entry_count, 10);
        assert_eq!(builder.block_idx, 1);

        for i in 10..20 {
            builder.add_from_kv(create_test_kv(&format!("key-{}", i), "value"))?;
        }
        assert_eq!(builder.entry_count, 20);
        assert_eq!(builder.block_idx, 1);
        builder.seal_current_block()?;
        assert_eq!(builder.entry_count, 20);
        assert_eq!(builder.block_idx, 2);
        Ok(())
    }
}
