use crate::KeyValue;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DeltaEncodedKV {
    shared_bytes: usize,
    unshared_bytes: usize,
    value_bytes: usize,
    key_delta: Box<[u8]>,
    value: Box<[u8]>,
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
}
