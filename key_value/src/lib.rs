mod key_value_pair;


#[derive(Debug, Default)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Default)]
pub struct FencePointer{
     n : usize,
     m : usize
}
