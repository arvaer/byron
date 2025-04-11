use std::cmp::Ordering;
use std::{collections::BinaryHeap, path::PathBuf, sync::Arc};

use key_value::KeyValue;
use sstable::{builder::SSTableFeatures, streamed_builder::StreamedSSTableBuilder, SSTable};
use tokio::{
    sync::mpsc::{self, channel, Receiver, Sender},
    task::{JoinHandle},
};
use uuid::Uuid;

use crate::{error::LsmError, level::Level};

#[derive(Debug)]
pub enum CompactionTask {
    CompactLevel {
        level: Level,
        base_fpr: f64,
        parent_directory: PathBuf,
        bloom_enabled: bool,
    },
    PowerOff,
}

#[derive(Debug)]
pub enum CompactionResult {
    Completed {
        compacted_table: Arc<SSTable>,
        original_level: usize,
        items_processed: usize,
    },
    Failed {
        error_value: LsmError,
        original_level: usize,
    },
}

#[derive(Debug)]
pub struct Walle {
    pub sender: mpsc::Sender<CompactionTask>,
    pub receiver: mpsc::Receiver<CompactionResult>,
    compaction_loop: Option<JoinHandle<Result<(), LsmError>>>,
}

impl Default for Walle {
    fn default() -> Self {
        let (task_sender, task_receiver) = channel::<CompactionTask>(32);
        let (result_sender, result_receiver) = channel::<CompactionResult>(32);

        let compaction_loop =
            tokio::spawn(async move { Self::compact_loop(task_receiver, result_sender).await });

        Self {
            sender: task_sender,
            receiver: result_receiver,
            compaction_loop: Some(compaction_loop),
        }
    }
}

impl Walle {
    pub fn new() -> Self {
        let (task_sender, task_receiver) = channel::<CompactionTask>(32);
        let (result_sender, result_receiver) = channel::<CompactionResult>(32);

        let compaction_loop =
            tokio::spawn(async move { Self::compact_loop(task_receiver, result_sender).await });

        Self {
            sender: task_sender,
            receiver: result_receiver,
            compaction_loop: Some(compaction_loop),
        }
    }
    pub async fn send_task(&self, task: CompactionTask) -> Result<(), LsmError> {
        self.sender
            .send(task)
            .await
            .map_err(|_| LsmError::Other("Failed to submit compaction task".to_string()))
    }

    pub async fn check_results(&mut self) -> Option<CompactionResult> {
        self.receiver.try_recv().ok()
    }

    pub async fn drain_results(&mut self) -> Vec<CompactionResult> {
        let mut results = Vec::new();

        while let Ok(result) = self.receiver.try_recv() {
            results.push(result);
        }

        results
    }

    async fn shut_down(&mut self) -> Result<(), LsmError> {
        self.send_task(CompactionTask::PowerOff).await?;
        if let Some(handle) = self.compaction_loop.take() {
            handle
                .await
                .map_err(|e| LsmError::Other(format!("Compaction thread panicked: {:?}", e)))??;
        }
        Ok(())
    }

    async fn compact_loop(
        mut receiver: Receiver<CompactionTask>,
        sender: Sender<CompactionResult>,
    ) -> Result<(), LsmError> {
        while let Some(task) = receiver.recv().await {
            match task {
                CompactionTask::CompactLevel {
                    level,
                    base_fpr,
                    parent_directory,
                    bloom_enabled,
                } => {
                    match Self::compact(level.clone(), base_fpr, parent_directory, bloom_enabled) {
                        Ok(compaction_result) => {
                            let _ = sender.send(compaction_result).await;
                        }
                        Err(e) => {
                            let _ = sender
                                .send(CompactionResult::Failed {
                                    error_value: e,
                                    original_level: level.depth,
                                })
                                .await;
                        }
                    }
                }
                CompactionTask::PowerOff => {
                    break;
                }
            }
        }

        Ok(())
    }

    fn compact(
        level: Level,
        base_fpr: f64,
        parent_directory: PathBuf,
        bloom_enabled: bool,
    ) -> Result<CompactionResult, LsmError> {
        println!("Beginning level cmpaction!");
        let features = SSTableFeatures {
            fpr: level.width as f64 * base_fpr,
            item_count: level.total_entries,
        };

        let file_name = parent_directory.join(format!("sstable-id-{}", Uuid::new_v4()));

        let mut min_heap: BinaryHeap<HeapItem> = BinaryHeap::new();
        let mut iters: Vec<_> = level.inner.iter().map(|table| table.iter()).collect();
        for (sstable_idx, iter) in iters.iter_mut().enumerate() {
            if let Some(kv_result) = iter.next() {
                let kv = kv_result?;
                min_heap.push(HeapItem {
                    key_value: kv,
                    sstable_idx,
                })
            }
        }

        println!("Construction new iters");
        let mut new_table = StreamedSSTableBuilder::new(features, bloom_enabled, &file_name)?;
        let mut items_processed = 0;

        while let Some(HeapItem {
            key_value,
            sstable_idx,
        }) = min_heap.pop()
        {
            let _ = new_table.add_from_kv(key_value);
            items_processed += 1;
            if let Some(next_kv) = iters[sstable_idx].next() {
                min_heap.push(HeapItem {
                    key_value: next_kv?,
                    sstable_idx,
                });
            }
        }

        println!("construction finished");

        let compacted_table = new_table.finalize()?;
        let sending_result = CompactionResult::Completed {
            compacted_table,
            original_level: level.depth,
            items_processed,
        };
        println!("created compaction result :{:?}", sending_result);

        // cleanup
        for table in level.inner {
            if let Err(e) = table.delete() {
                log::warn!("Failed to delete SSTable: {:?}", e);
            }
        }

        println!("deleted tables and sending");
        Ok(sending_result)
    }
}

struct HeapItem {
    key_value: KeyValue,
    sstable_idx: usize,
}

impl Eq for HeapItem {}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key_value == other.key_value
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other.key_value.cmp(&self.key_value)
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
