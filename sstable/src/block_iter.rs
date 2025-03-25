use std::{io::{Cursor, Read}, sync::Arc};
use integer_encoding::VarIntReader;
use key_value::{key_value_pair::DeltaEncodedKV, KeyValue};

pub struct SSTableBlockIterator {
    offset: usize,
    previous: Option<KeyValue>,
    block: Arc<[u8]>
}

impl  SSTableBlockIterator {
    pub fn new(block:Arc<[u8]>) -> Self {
        Self {
            offset: 0,
            previous: None,
            block
        }
    }
}



impl Iterator for SSTableBlockIterator {
    type Item = KeyValue;
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.block.len() {
            return None;
        }
        let mut cursor = Cursor::new(&self.block[self.offset..]);
        let start_offset = self.offset;
        let shared_bytes = cursor.read_varint().ok()?;
        let unshared_bytes = cursor.read_varint().ok()?;
        let value_bytes = cursor.read_varint().ok()?;

        let remaining = self.block.len() - start_offset;
        if remaining < (cursor.position() as usize + unshared_bytes + value_bytes) {
            return None;
        }

        let mut key_delta = vec![0u8; unshared_bytes];
        if cursor.read_exact(&mut key_delta).is_err() {
            return None;
        }
        let mut value = vec![0u8; value_bytes];
        if cursor.read_exact(&mut value).is_err() {
            return None;
        }

        self.offset += cursor.position() as usize;

        let dkv = DeltaEncodedKV {
            shared_bytes,
            unshared_bytes,
            value_bytes,
            key_delta: key_delta.into_boxed_slice(),
            value: value.into_boxed_slice(),
        };
        let current = dkv.reverse(self.previous.clone())?;

        self.previous = Some(current.clone());
        Some(current)
    }
}
