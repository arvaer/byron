#[derive(Debug, Clone)]
pub struct Level {
    pub inner: Vec<std::sync::Arc<sstable::SSTable>>,
    pub depth: usize,
    pub width: usize,
    pub total_entries: usize,
}
