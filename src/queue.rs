use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use anyhow::Result;
use crdts::{lwwreg::LWWReg, CvRDT};
use serde::{Deserialize, Serialize};
use slab::Slab;
#[cfg(fuzzing)]
use tokio::runtime::Builder as TokioRuntimeBuilder;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Compute,
    Io,
}

impl fmt::Display for TaskKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskKind::Compute => write!(f, "compute"),
            TaskKind::Io => write!(f, "io"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Hash)]
pub struct Task {
    pub id: u64,
    pub kind: TaskKind,
    pub priority: u8,
    pub payload: Vec<u8>,
}

impl Task {
    pub fn new(id: u64, kind: TaskKind, priority: u8, payload: Vec<u8>) -> Self {
        Self {
            id,
            kind,
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

    pub fn checksum(&self) -> u64 {
        self.payload
            .iter()
            .fold(0u64, |acc, byte| acc.wrapping_add(u64::from(*byte)))
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum QueueLogEntry {
    Enqueue { task: Task },
    Dequeue { task_ids: Vec<u64> },
    Snapshot { payload: Vec<u8> },
}

pub trait QueueReplicator: Send + Sync {
    fn replicate(&self, entry: QueueLogEntry);
}

#[derive(Clone, Default)]
struct NoopReplicator;

impl QueueReplicator for NoopReplicator {
    fn replicate(&self, _entry: QueueLogEntry) {}
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
struct QueueSnapshot {
    actor: String,
    tasks: Vec<Task>,
}

#[derive(Clone, Debug)]
struct QueuedEntry {
    priority: u8,
    id: u64,
    handle: usize,
}

impl QueuedEntry {
    fn new(handle: usize, task: &Task) -> Self {
        Self {
            priority: task.priority,
            id: task.id,
            handle,
        }
    }
}

impl Eq for QueuedEntry {}

impl PartialEq for QueuedEntry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Ord for QueuedEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for QueuedEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
struct TaskArena {
    storage: Slab<Task>,
}

impl TaskArena {
    fn new() -> Self {
        Self {
            storage: Slab::new(),
        }
    }

    fn insert(&mut self, task: Task) -> usize {
        self.storage.insert(task)
    }

    fn get(&self, handle: usize) -> Option<&Task> {
        self.storage.get(handle)
    }

    fn remove(&mut self, handle: usize) -> Option<Task> {
        if self.storage.get(handle).is_some() {
            Some(self.storage.remove(handle))
        } else {
            None
        }
    }

    fn values(&self) -> Vec<Task> {
        self.storage.iter().map(|(_, task)| task.clone()).collect()
    }

    fn clear(&mut self) {
        self.storage.clear();
    }
}

#[derive(Default)]
struct QueueState {
    heap: BinaryHeap<QueuedEntry>,
    arena: TaskArena,
    id_index: HashMap<u64, usize>,
}

impl QueueState {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            arena: TaskArena::new(),
            id_index: HashMap::new(),
        }
    }

    fn push(&mut self, task: Task) -> bool {
        if self.id_index.contains_key(&task.id) {
            return false;
        }
        let handle = self.arena.insert(task.clone());
        self.heap.push(QueuedEntry::new(handle, &task));
        self.id_index.insert(task.id, handle);
        true
    }

    fn pop(&mut self) -> Option<Task> {
        while let Some(entry) = self.heap.pop() {
            if let Some(task) = self.arena.remove(entry.handle) {
                self.id_index.remove(&entry.id);
                return Some(task);
            }
            self.id_index.remove(&entry.id);
        }
        None
    }

    fn pop_batch(&mut self, max: usize, filter: Option<TaskKind>) -> Vec<Task> {
        let mut batch = Vec::new();
        let mut skipped = Vec::new();

        while batch.len() < max {
            match self.heap.pop() {
                Some(entry) => match self.arena.get(entry.handle) {
                    Some(task_ref) => {
                        if filter
                            .as_ref()
                            .map_or(true, |expected| &task_ref.kind == expected)
                        {
                            let task = self.arena.remove(entry.handle).expect("task present");
                            self.id_index.remove(&entry.id);
                            batch.push(task);
                        } else {
                            skipped.push(entry);
                        }
                    }
                    None => {
                        self.id_index.remove(&entry.id);
                    }
                },
                None => break,
            }
        }

        for entry in skipped {
            self.heap.push(entry);
        }

        batch
    }

    fn remove_many(&mut self, task_ids: &[u64]) {
        if task_ids.is_empty() {
            return;
        }

        let mut removed_handles = HashSet::new();
        for task_id in task_ids {
            if let Some(handle) = self.id_index.remove(task_id) {
                if self.arena.remove(handle).is_some() {
                    removed_handles.insert(handle);
                }
            }
        }

        if removed_handles.is_empty() {
            return;
        }

        let mut retained = BinaryHeap::new();
        while let Some(entry) = self.heap.pop() {
            if removed_handles.contains(&entry.handle) {
                continue;
            }
            retained.push(entry);
        }
        self.heap = retained;
    }

    fn tasks(&self) -> Vec<Task> {
        self.arena.values()
    }

    fn replace(&mut self, tasks: Vec<Task>) {
        self.heap = BinaryHeap::new();
        self.arena.clear();
        self.id_index.clear();
        for task in tasks {
            self.push(task);
        }
    }

    fn len(&self) -> usize {
        self.id_index.len()
    }
}

#[derive(Clone)]
pub struct TaskQueue {
    inner: Arc<RwLock<QueueState>>,
    replicator: Arc<dyn QueueReplicator>,
    snapshot: Arc<RwLock<LWWReg<QueueSnapshot, u64>>>,
    clock: Arc<AtomicU64>,
    actor_label: String,
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskQueue {
    pub fn new() -> Self {
        Self::with_actor("default")
    }

    pub fn with_actor(actor_label: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(QueueState::new())),
            replicator: Arc::new(NoopReplicator),
            snapshot: Arc::new(RwLock::new(LWWReg::default())),
            clock: Arc::new(AtomicU64::new(0)),
            actor_label: actor_label.into(),
        }
    }

    pub fn with_replicator(replicator: Arc<dyn QueueReplicator>) -> Self {
        Self::with_replicator_and_actor(replicator, "default")
    }

    pub fn with_replicator_and_actor(
        replicator: Arc<dyn QueueReplicator>,
        actor_label: impl Into<String>,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(QueueState::new())),
            replicator,
            snapshot: Arc::new(RwLock::new(LWWReg::default())),
            clock: Arc::new(AtomicU64::new(0)),
            actor_label: actor_label.into(),
        }
    }

    pub fn set_replicator(&mut self, replicator: Arc<dyn QueueReplicator>) {
        self.replicator = replicator;
    }

    pub async fn push(&self, task: Task) {
        let mut guard = self.inner.write().await;
        if guard.push(task.clone()) {
            self.replicate(QueueLogEntry::Enqueue { task });
            drop(guard);
            self.refresh_snapshot(true).await;
        }
    }

    pub async fn pop(&self) -> Option<Task> {
        let mut guard = self.inner.write().await;
        let result = guard.pop();
        if let Some(ref task) = result {
            self.replicate(QueueLogEntry::Dequeue {
                task_ids: vec![task.id],
            });
            drop(guard);
            self.refresh_snapshot(true).await;
        }
        result
    }

    pub async fn len(&self) -> usize {
        let guard = self.inner.read().await;
        guard.len()
    }

    pub async fn pop_batch(&self, max: usize, filter: Option<TaskKind>) -> Vec<Task> {
        let mut guard = self.inner.write().await;
        let batch = guard.pop_batch(max, filter.clone());
        if !batch.is_empty() {
            let ids = batch.iter().map(|task| task.id).collect();
            self.replicate(QueueLogEntry::Dequeue { task_ids: ids });
            drop(guard);
            self.refresh_snapshot(true).await;
        } else {
            drop(guard);
        }
        batch
    }

    pub async fn apply_log(&self, entry: QueueLogEntry) {
        match entry {
            QueueLogEntry::Enqueue { task } => {
                {
                    let mut guard = self.inner.write().await;
                    guard.push(task);
                }
                self.refresh_snapshot(false).await;
            }
            QueueLogEntry::Dequeue { task_ids } => {
                if !task_ids.is_empty() {
                    {
                        let mut guard = self.inner.write().await;
                        guard.remove_many(&task_ids);
                    }
                    self.refresh_snapshot(false).await;
                }
            }
            QueueLogEntry::Snapshot { payload } => {
                if let Ok(remote) = postcard::from_bytes::<LWWReg<QueueSnapshot, u64>>(&payload) {
                    {
                        let mut snapshot = self.snapshot.write().await;
                        snapshot.merge(remote);
                    }
                    self.restore_from_snapshot().await;
                }
            }
        }
    }

    fn replicate(&self, entry: QueueLogEntry) {
        self.replicator.replicate(entry);
    }

    async fn refresh_snapshot(&self, broadcast: bool) {
        let tasks = {
            let guard = self.inner.read().await;
            guard.tasks()
        };

        let marker = self.clock.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        {
            let mut snapshot = self.snapshot.write().await;
            snapshot.update(
                QueueSnapshot {
                    actor: self.actor_label.clone(),
                    tasks,
                },
                marker,
            );
        }

        #[cfg(not(feature = "raft-consensus"))]
        if broadcast {
            self.broadcast_snapshot().await;
        }
    }

    async fn broadcast_snapshot(&self) {
        let payload = {
            let snapshot = self.snapshot.read().await;
            postcard::to_allocvec(&*snapshot)
        };

        if let Ok(data) = payload {
            self.replicate(QueueLogEntry::Snapshot { payload: data });
        }
    }

    async fn restore_from_snapshot(&self) {
        let snapshot = self.snapshot.read().await;
        let tasks = snapshot.val.tasks.clone();
        let marker = snapshot.marker;
        drop(snapshot);

        self.clock.fetch_max(marker, AtomicOrdering::SeqCst);

        let mut guard = self.inner.write().await;
        guard.replace(tasks);
    }

    #[cfg_attr(not(test), doc(hidden))]
    pub async fn export_snapshot(&self) -> Vec<u8> {
        let snapshot = self.snapshot.read().await;
        postcard::to_allocvec(&*snapshot).unwrap()
    }

    #[cfg_attr(not(test), doc(hidden))]
    pub async fn import_snapshot(&self, payload: Vec<u8>) {
        if let Ok(remote) = postcard::from_bytes::<LWWReg<QueueSnapshot, u64>>(&payload) {
            {
                let mut snapshot = self.snapshot.write().await;
                snapshot.merge(remote);
            }
            self.restore_from_snapshot().await;
        }
    }
}

#[cfg(fuzzing)]
pub fn fuzz_apply(entries: Vec<QueueLogEntry>) {
    let runtime = TokioRuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("fuzz runtime");
    runtime.block_on(async move {
        let queue = TaskQueue::new();
        for entry in entries {
            queue.apply_log(entry).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn tasks_are_serializable(id in any::<u64>(), priority in any::<u8>(), payload in proptest::collection::vec(any::<u8>(), 1..128)) {
            let task = Task::new(id, TaskKind::Compute, priority, payload);
            let encoded = task.encode_bincode().unwrap();
            let decoded: Task = bincode::deserialize(&encoded).unwrap();
            prop_assert_eq!(task.id, decoded.id);
            prop_assert_eq!(task.priority, decoded.priority);
        }

        #[test]
        fn snapshot_roundtrip(tasks in proptest::collection::vec((any::<u64>(), any::<u8>(), proptest::collection::vec(any::<u8>(), 1..32)), 1..16)) {
            let queue = TaskQueue::new();
            let mut original = Vec::new();
            for (id, priority, payload) in tasks {
                let task = Task::new(id, TaskKind::Compute, priority, payload);
                original.push(task.clone());
                futures::executor::block_on(queue.push(task));
            }

            let snapshot_bytes = futures::executor::block_on(async {
                let snapshot = queue.snapshot.read().await;
                postcard::to_allocvec(&*snapshot).unwrap()
            });

            let replica = TaskQueue::new();
            futures::executor::block_on(replica.apply_log(QueueLogEntry::Snapshot { payload: snapshot_bytes }));
            let restored_len = futures::executor::block_on(replica.len());
            prop_assert_eq!(restored_len, original.len());
        }
    }
}

#[cfg(all(test, feature = "loom-tests"))]
mod loom_queue_tests {
    use super::*;
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    #[test]
    fn arena_concurrency() {
        loom::model(|| {
            let arena = Arc::new(Mutex::new(TaskArena::new()));
            let arena1 = arena.clone();
            let t1 = thread::spawn(move || {
                let mut arena = arena1.lock().unwrap();
                arena.insert(Task::new(1, TaskKind::Compute, 1, vec![1]));
            });
            let arena2 = arena.clone();
            let t2 = thread::spawn(move || {
                let mut arena = arena2.lock().unwrap();
                arena.insert(Task::new(2, TaskKind::Io, 1, vec![2]));
            });
            t1.join().unwrap();
            t2.join().unwrap();
        });
    }
}
