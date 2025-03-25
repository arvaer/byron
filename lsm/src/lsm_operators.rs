use std::sync::Arc;

use key_value::KeyValue;

use crate::error::LsmError;

pub trait LsmSearchOperators {
    fn get(&self, key: String) -> Result<Arc<KeyValue>, LsmError>;
    fn put(&mut self, key: String, value: String);
    fn range();
}
