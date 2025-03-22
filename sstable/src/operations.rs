use crate::SSTable;

pub trait SSTableOps{
    fn create();
    fn iter();
    fn size();
    fn features();
}
