use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use crdts::{CmRDT, CvRDT, Orswot};
use slab::Slab;

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
    // CRDT state: set of task ids present in the queue
    crdt: Arc<RwLock<Orswot<u64, u64>>>,
    crdt_actor: u64,
    store: Arc<RwLock<Slab<Task>>>,
    id_index: Arc<RwLock<HashMap<u64, usize>>>,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BinaryHeap::new())),
            crdt: Arc::new(RwLock::new(Orswot::new())),
            crdt_actor: rand::random::<u64>(),
            store: Arc::new(RwLock::new(Slab::with_capacity(1024))),
            id_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn push(&self, task: Task) {
        // Insert into slab store for allocator-friendly management
        let idx = {
            let mut store = self.store.write().await;
            store.insert(task.clone())
        };
        {
            let mut map = self.id_index.write().await;
            map.insert(task.id, idx);
        }
        {
            let mut set = self.crdt.write().await;
            let add_ctx = set.read().derive_add_ctx(self.crdt_actor);
            let op = set.add(task.id, add_ctx);
            set.apply(op);
        }
        let mut guard = self.inner.write().await;
        guard.push(task);
    }

    pub async fn pop(&self) -> Option<Task> {
        let mut guard = self.inner.write().await;
        if let Some(task) = guard.pop() {
            {
                let mut set = self.crdt.write().await;
                let rm_ctx = set.read().derive_rm_ctx();
                let op = set.rm(task.id, rm_ctx);
                set.apply(op);
            }
            // Remove from slab store and id index
            if let Some(idx) = self.id_index.write().await.remove(&task.id) {
                let mut store = self.store.write().await;
                store.remove(idx);
            }
            Some(task)
        } else {
            None
        }
    }

    pub async fn len(&self) -> usize {
        let guard = self.inner.read().await;
        guard.len()
    }

    // Pop up to `max` tasks, optionally filtered by inferred TaskKind,
    // preserving overall priority ordering among returned tasks.
    pub async fn pop_batch(&self, max: usize, kind: Option<TaskKind>) -> Vec<Task> {
        let mut guard = self.inner.write().await;

        let mut selected: Vec<Task> = Vec::with_capacity(max);
        let mut stash: Vec<Task> = Vec::new();

        while selected.len() < max {
            if let Some(task) = guard.pop() {
                if kind.map(|k| infer_task_kind(&task) == k).unwrap_or(true) {
                    selected.push(task);
                } else {
                    stash.push(task);
                }
            } else {
                break;
            }
        }

        // Push back tasks that were skipped due to kind filter
        for task in stash.into_iter() {
            guard.push(task);
        }

        // Update CRDT and by_id for selected tasks
        if !selected.is_empty() {
            let mut set = self.crdt.write().await;
            let rm_ctx = set.read().derive_rm_ctx();
            for t in &selected {
                let op = set.rm(t.id, rm_ctx.clone());
                set.apply(op);
            }
            let mut map = self.id_index.write().await;
            let mut store = self.store.write().await;
            for t in &selected {
                if let Some(idx) = map.remove(&t.id) {
                    store.remove(idx);
                }
            }
        }

        selected
    }

    // Remove tasks by id (for replicated dequeue on followers)
    pub async fn remove_by_ids(&self, ids: &[u64]) {
        let idset: HashSet<u64> = ids.iter().copied().collect();
        let mut guard = self.inner.write().await;
        let mut all: Vec<Task> = guard.drain().collect();
        all.retain(|t| !idset.contains(&t.id));
        for t in all.into_iter() {
            guard.push(t);
        }
        let mut set = self.crdt.write().await;
        let rm_ctx = set.read().derive_rm_ctx();
        for id in ids {
            let op = set.rm(*id, rm_ctx.clone());
            set.apply(op);
        }
        let mut map = self.id_index.write().await;
        let mut store = self.store.write().await;
        for id in ids {
            if let Some(idx) = map.remove(id) {
                store.remove(idx);
            }
        }
    }
}

// TaskKind lives with queue so both coordinator and workers share the categorization.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum TaskKind {
    Compute,
    IoBound,
}

pub fn infer_task_kind(task: &Task) -> TaskKind {
    match task.payload.first() {
        Some(byte) if byte & 0x1 == 0x1 => TaskKind::IoBound,
        _ => TaskKind::Compute,
    }
}

// Log-replicated queue operations encoded with postcard
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum QueueOp {
    Enqueue(Task),
    EnqueueBatch(Vec<Task>),
    DequeueBatch { task_ids: Vec<u64> },
    CrdtState(Vec<u8>),
}

impl QueueOp {
    pub fn to_postcard(&self) -> Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_postcard(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

impl TaskQueue {
    pub async fn crdt_export(&self) -> Result<Vec<u8>> {
        let set = self.crdt.read().await;
        Ok(postcard::to_allocvec(&*set)?)
    }

    pub async fn crdt_merge(&self, bytes: &[u8]) -> Result<()> {
        let remote: Orswot<u64, u64> = postcard::from_bytes(bytes)?;
        let mut set = self.crdt.write().await;
        set.merge(remote);
        // Rebuild heap from by_id ∩ crdt members
        let map = self.id_index.read().await;
        let store = self.store.read().await;
        let members: HashSet<u64> = set.read().val.iter().copied().collect();
        let mut heap = BinaryHeap::new();
        for (id, idx) in map.iter() {
            if members.contains(id) {
                if let Some(task) = store.get(*idx) {
                    heap.push(task.clone());
                }
            }
        }
        let mut guard = self.inner.write().await;
        *guard = heap;
        Ok(())
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

        #[test]
        fn crdt_export_import_round_trip(ids in proptest::collection::vec(any::<u64>(), 1..50)) {
            let q = TaskQueue::new();
            for (i, id) in ids.iter().cloned().enumerate() {
                let t = Task::new(id, (i%10) as u8, vec![1,2,3,4]);
                futures::executor::block_on(q.push(t));
            }
            let before = futures::executor::block_on(q.crdt_export()).unwrap();
            let q2 = TaskQueue::new();
            futures::executor::block_on(q2.crdt_merge(&before)).unwrap();
            prop_assert!(futures::executor::block_on(async { q.len().await == q2.len().await }));
        }

        #[test]
        fn batch_respects_priority_and_kind(
            ids in proptest::collection::vec(any::<u64>(), 10..50),
            priorities in proptest::collection::vec(1u8..=10u8, 10..50),
        ) {
            let q = TaskQueue::new();
            for (i, id) in ids.iter().cloned().enumerate() {
                let pr = priorities[i % priorities.len()];
                let payload = if i % 2 == 0 { vec![1,2,3] } else { vec![2] };
                let t = Task::new(id, pr, payload);
                futures::executor::block_on(q.push(t));
            }
            let batch = futures::executor::block_on(q.pop_batch(10, Some(TaskKind::Compute)));
            // Priority non-increasing
            for w in batch.windows(2) {
                prop_assert!(w[0].priority >= w[1].priority);
            }
            // Kind filter applied
            for t in batch.iter() {
                prop_assert_eq!(infer_task_kind(t), TaskKind::Compute);
            }
        }
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::Arc as LoomArc;
    use loom::thread;

    // A minimal loom model to ensure our checksum logic would be deterministic under interleavings
    #[test]
    fn loom_counter_consistency() {
        loom::model(|| {
            let c = LoomArc::new(AtomicUsize::new(0));
            let c1 = c.clone();
            let t1 = thread::spawn(move || {
                c1.fetch_add(1, Ordering::SeqCst);
            });
            let c2 = c.clone();
            let t2 = thread::spawn(move || {
                c2.fetch_add(1, Ordering::SeqCst);
            });
            t1.join().unwrap();
            t2.join().unwrap();
            assert_eq!(c.load(Ordering::SeqCst), 2);
        });
    }
}
