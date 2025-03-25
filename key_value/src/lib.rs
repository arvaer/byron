pub mod key_value_pair;


#[derive(Debug, Default, Clone, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

