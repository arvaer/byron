use std::{borrow::BorrowMut, pin::Pin};

use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone)]
pub struct Level {
    pub inner: Vec<std::sync::Arc<sstable::SSTable>>,
    pub depth: usize,
    pub width: usize,
    pub total_entries: usize,
}

#[derive(Debug)]
pub struct LevelMutex {
    pub inner: Mutex<Level>,
    pub notify_insertion_complete: Notify,
}

// with pinned level, I basically want to use the notify to know when I should clear the level out.
// This should only occur once i've inserted the next level.
// So a levels update function
impl LevelMutex {
    pub fn new(level: Level) -> Self {
        LevelMutex {
            inner: Mutex::new(level),
            notify_insertion_complete: Notify::new(),
        }
    }

    // this function will block until the insertion is complete
    pub async fn wait_for_insertion(&self) {
        self.notify_insertion_complete.notified().await;
    }

    // this function will apply the function to the level
    pub async fn update_inner<F>(&self, func: F)
    where
        F: FnOnce(&mut Level),
    {
        let mut inner_guard = self.inner.lock().await;
        func(inner_guard.borrow_mut());
        drop(inner_guard);
        self.notify_insertion_complete.notify_waiters();
    }

    // lightweight reads do we just hold a ref
    pub async fn read(&self) -> Level {
        let guard = self.inner.lock().await;
        (*guard).clone()
    }

    pub async fn clear(&self) {
        self.update_inner(|level| {
            level.inner.clear();
            level.total_entries = 0;
        })
        .await;
    }

    pub async fn is_empty(&self) -> bool {
        self.read().await.inner.is_empty()
    }
    pub async fn get_len(&self) -> usize{
        self.read().await.inner.len()
    }
    pub async fn get_width(&self) -> usize{
        self.read().await.width
    }
    pub async fn get_depth(&self) -> usize{
        self.read().await.depth
    }
    pub async fn get_entries(&self) -> usize{
        self.read().await.total_entries
    }
}

