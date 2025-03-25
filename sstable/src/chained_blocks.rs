use key_value::KeyValue;

use crate::{block_iter::SSTableBlockIterator, error::SSTableError, SSTable};

pub struct SSTableIterator<'a> {
    sstable: &'a SSTable,
    current_block : usize,
    total_blocks: usize,
    block_iter: Option<SSTableBlockIterator>
}

impl<'a> SSTableIterator<'a> {
    pub fn new(sstable: &'a SSTable) -> Self {
        let total_blocks = sstable.fence_pointers.len();
        SSTableIterator{
            sstable,
            current_block: 0,
            total_blocks,
            block_iter: None
        }
    }
}


impl<'a> Iterator for SSTableIterator<'a> {
    type Item = Result<KeyValue, SSTableError>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut iter) = self.block_iter {
                if let Some(kv) = iter.next() {
                    return Some(Ok(kv))
                }
            }

            if self.current_block >= self.total_blocks {
                return None;
            }

            let block_id = (self.current_block, self.current_block + 1);
            self.current_block += 1;
            match self.sstable.iter_block(block_id) {
                Ok(block_iterator) => self.block_iter = Some(block_iterator),
                Err(e) => return Some(Err(e))
            }
        }
    }
}
