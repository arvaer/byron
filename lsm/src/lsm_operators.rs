use std::sync::Arc;

use key_value::{key_value_pair, KeyValue};
use memtable::MemTableOperations;

use crate::{error::LsmError, lsm_database::LsmDatabase};

pub trait LsmSearchOperators {
    fn get(&self, key: String) -> Result<Arc<KeyValue>, LsmError>;
    fn put(&mut self, key: String, value: String);
    fn range();
}
