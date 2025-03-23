use std::sync::Arc;

use crate::KeyValue;
use integer_encoding::*;

#[derive(Debug, Clone, PartialEq, Default, Eq, Hash)]
//
pub struct DeltaEncodedKV {
    pub shared_bytes: usize,
    pub unshared_bytes: usize,
    pub value_bytes: usize,
    pub key_delta: Box<[u8]>,
    pub value: Box<[u8]>,
}

impl DeltaEncodedKV {
    pub fn forward(previous_kv: Option<KeyValue>, kv: KeyValue) -> Self {
        let value_bytes: usize = kv.value.len();
        let value: Box<[u8]> = Box::from(kv.value.as_bytes());

        if let Some(pkv) = previous_kv {
            let mut shared_bytes: usize = 0;

            let current_key_bytes = kv.key.as_bytes();
            let previous_key_bytes = pkv.key.as_bytes();

            let min_len = std::cmp::min(current_key_bytes.len(), previous_key_bytes.len());

            for i in 0..min_len {
                if current_key_bytes[i] == previous_key_bytes[i] {
                    shared_bytes += 1;
                } else {
                    break;
                }
            }

            //            let key_delta: Box<[u8]> = Box::from(&current_key_bytes[shared_bytes..]);
            let key_delta: Box<[u8]> = current_key_bytes[shared_bytes..].into();
            let unshared_bytes = kv.key.len() - shared_bytes;

            Self {
                shared_bytes,
                unshared_bytes,
                value_bytes,
                key_delta,
                value,
            }
        } else {
            let shared_bytes: usize = 0;
            let unshared_bytes: usize = kv.key.len();
            let key_delta: Box<[u8]> = Box::from(kv.key.as_bytes());

            Self {
                shared_bytes,
                unshared_bytes,
                value_bytes,
                key_delta,
                value,
            }
        }
    }

    pub fn reverse(&self, previous_kv: Option<KeyValue>) -> Option<KeyValue> {
        let value = String::from(std::str::from_utf8(&self.value).unwrap());
        let suffix = std::str::from_utf8(&self.key_delta).unwrap();
        if let Some(pkv) = previous_kv {
            let prefix = String::from(
                std::str::from_utf8(&pkv.key.as_bytes()[..self.shared_bytes]).unwrap(),
            );
            let full_key = prefix + suffix;
            Some(KeyValue {
                key: full_key,
                value,
            })
        } else {
            Some(KeyValue {
                key: String::from(suffix),
                value,
            })
        }
    }

    pub fn calculate_size(&self) -> usize {
        let mut size = 0;
        size += self.value_bytes.required_space();
        size += self.unshared_bytes.required_space();
        size += self.shared_bytes.required_space();
        size += self.key_delta.len();
        size += self.value.len();

        size
    }

    pub fn to_str(&self) -> Arc<[u8]> {
        let mut buffer = Vec::with_capacity(
            self.shared_bytes.required_space()
                + self.unshared_bytes.required_space()
                + self.value_bytes.required_space()
                + self.key_delta.len()
                + self.value.len(),
        );

        buffer.extend_from_slice(&self.shared_bytes.encode_var_vec());
        buffer.extend_from_slice(&self.unshared_bytes.encode_var_vec());
        buffer.extend_from_slice(&self.value_bytes.encode_var_vec());
        buffer.extend_from_slice(&self.key_delta);
        buffer.extend_from_slice(&self.value);

        let arc: Arc<[u8]> = Arc::from(buffer.into_boxed_slice());
        arc
    }
}
