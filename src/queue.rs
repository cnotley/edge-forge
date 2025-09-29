use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Task {
    pub id: u64,
    pub priority: u8,
    pub payload: Vec<u8>,
}

impl Task {
    pub fn new(id: u64, priority: u8, payload: Vec<u8>) -> Self {
        Self {
            id,
            priority,
            payload,
        }
    }

    pub fn encode_bincode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn encode_postcard(&self) -> Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }
}

impl Eq for Task {}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Ord for Task {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for Task {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Default)]
pub struct TaskQueue {
    inner: Arc<RwLock<BinaryHeap<Task>>>,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BinaryHeap::new())),
        }
    }

    pub async fn push(&self, task: Task) {
        let mut guard = self.inner.write().await;
        guard.push(task);
    }

    pub async fn pop(&self) -> Option<Task> {
        let mut guard = self.inner.write().await;
        guard.pop()
    }

    pub async fn len(&self) -> usize {
        let guard = self.inner.read().await;
        guard.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn tasks_are_serializable(id in any::<u64>(), priority in any::<u8>(), payload in proptest::collection::vec(any::<u8>(), 1..128)) {
            let task = Task::new(id, priority, payload);
            let encoded = task.encode_bincode().unwrap();
            let decoded: Task = bincode::deserialize(&encoded).unwrap();
            prop_assert_eq!(task.id, decoded.id);
            prop_assert_eq!(task.priority, decoded.priority);
        }
    }
}
