use crate::coordinator::{COORDINATOR_PORT, SERVICE_NAME};
use crate::queue::{Task, TaskKind};

use anyhow::{anyhow, Context, Result};
use backoff::backoff::Backoff;
use backoff::future::retry;
use backoff::{Error as BackoffError, ExponentialBackoff, ExponentialBackoffBuilder};
use futures_util::StreamExt;
use mdns_sd::{ServiceDaemon, ServiceEvent};
use rayon::prelude::*;
use rayon::ThreadPool;
use rayon::ThreadPoolBuilder;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use slab::Slab;
use slog::Logger;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tract_core::prelude::*;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SENSOR_DIR: &str = "/tmp/edgeforge";
const MAX_BACKOFF_DELAY: Duration = Duration::from_secs(5);
const ERROR_BACKOFF_INITIAL: Duration = Duration::from_millis(500);
const IDLE_BACKOFF_INITIAL: Duration = Duration::from_millis(200);
pub const DEFAULT_BATCH_SIZE: usize = 5;

#[derive(Clone)]
pub struct WorkerConfig {
    pub id: String,
    pub service_name: String,
    pub coordinator_hint: Option<String>,
    pub capability: TaskKind,
    pub batch_size: usize,
}

impl WorkerConfig {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            service_name: SERVICE_NAME.to_string(),
            coordinator_hint: None,
            capability: TaskKind::Compute,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn with_coordinator_hint(mut self, hint: impl Into<String>) -> Self {
        self.coordinator_hint = Some(hint.into());
        self
    }

    pub fn with_service_name(mut self, service: impl Into<String>) -> Self {
        self.service_name = service.into();
        self
    }

    pub fn with_capability(mut self, capability: TaskKind) -> Self {
        self.capability = capability;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1).min(10);
        self
    }
}

#[derive(Clone)]
pub struct Worker {
    inner: Arc<WorkerInner>,
}

struct WorkerInner {
    config: WorkerConfig,
    logger: Logger,
    http: Client,
    thread_pool: Arc<ThreadPool>,
    task_pool: Mutex<Slab<Task>>,
    notifier: Arc<Notify>,
    ws_task: Mutex<Option<JoinHandle<()>>>,
}

impl Worker {
    pub fn new(config: WorkerConfig, logger: Logger) -> Result<Self> {
        let http = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("failed to build worker HTTP client")?;

        let pool = ThreadPoolBuilder::new()
            .thread_name(|idx| format!("edgeforge-worker-{idx}"))
            .build()
            .context("failed to construct rayon thread pool")?;

        Ok(Self {
            inner: Arc::new(WorkerInner {
                config,
                logger,
                http,
                thread_pool: Arc::new(pool),
                task_pool: Mutex::new(Slab::new()),
                notifier: Arc::new(Notify::new()),
                ws_task: Mutex::new(None),
            }),
        })
    }

    /// Spawn the worker run loop on the Tokio runtime.
    pub fn spawn(self) -> JoinHandle<()> {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            if let Err(error) = Worker::drive(inner.clone()).await {
                slog::error!(inner.logger, "worker stopped";
                    "worker_id" => inner.config.id.clone(),
                    "error" => %error);
            }
        })
    }

    /// Run the worker loop on the current task, returning on error.
    pub async fn run_forever(self) -> Result<()> {
        Worker::drive(self.inner.clone()).await
    }

    async fn drive(inner: Arc<WorkerInner>) -> Result<()> {
        let mut base_url = match inner.coordinator_base_url().await {
            Ok(url) => url,
            Err(error) => {
                slog::warn!(inner.logger, "falling back to localhost coordinator";
                    "worker_id" => inner.config.id.clone(),
                    "error" => %error);
                Url::parse(&format!("http://127.0.0.1:{COORDINATOR_PORT}/"))
                    .expect("valid bootstrap URL")
            }
        };

        inner.clone().spawn_ws_listener(base_url.clone()).await;

        slog::info!(inner.logger, "worker online";
            "worker_id" => inner.config.id.clone(),
            "coordinator" => base_url.as_str(),
            "capability" => inner.config.capability.to_string(),
            "batch_size" => inner.config.batch_size);

        let mut idle_backoff = WorkerInner::idle_backoff();
        let mut error_backoff = WorkerInner::error_backoff();

        loop {
            match inner.fetch_tasks_with_retry(&base_url).await {
                Ok(tasks) if !tasks.is_empty() => {
                    idle_backoff.reset();
                    error_backoff.reset();

                    let handles = {
                        let mut pool = inner.task_pool.lock().await;
                        tasks
                            .into_iter()
                            .map(|task| pool.insert(task))
                            .collect::<Vec<_>>()
                    };

                    let tasks = inner.drain_handles(handles).await;
                    let (compute_tasks, io_tasks): (Vec<_>, Vec<_>) = tasks
                        .into_iter()
                        .partition(|task| matches!(task.kind, TaskKind::Compute));

                    if !compute_tasks.is_empty() {
                        let results = inner.process_compute_batch(compute_tasks);
                        for (task, outcome) in results {
                            match outcome {
                                Ok(outcome) => {
                                    inner.log_task_result(&task, &outcome);
                                    if let Err(error) =
                                        inner.report_completion(&base_url, &task, &outcome).await
                                    {
                                        slog::warn!(inner.logger, "failed to report completion";
                                            "worker_id" => inner.config.id.clone(),
                                            "task_id" => task.id,
                                            "error" => %error);
                                    }
                                }
                                Err(error) => {
                                    slog::error!(inner.logger, "task processing failed";
                                        "worker_id" => inner.config.id.clone(),
                                        "task_id" => task.id,
                                        "error" => %error);
                                }
                            }
                        }
                    }

                    for task in io_tasks {
                        match inner.process_io_task(&task).await {
                            Ok(outcome) => {
                                inner.log_task_result(&task, &outcome);
                                if let Err(error) =
                                    inner.report_completion(&base_url, &task, &outcome).await
                                {
                                    slog::warn!(inner.logger, "failed to report completion";
                                        "worker_id" => inner.config.id.clone(),
                                        "task_id" => task.id,
                                        "error" => %error);
                                }
                            }
                            Err(error) => {
                                slog::error!(inner.logger, "task processing failed";
                                    "worker_id" => inner.config.id.clone(),
                                    "task_id" => task.id,
                                    "error" => %error);
                            }
                        }
                    }
                }
                Ok(_) => {
                    if let Some(delay) = idle_backoff.next_backoff() {
                        tokio::select! {
                            _ = sleep(delay) => {},
                            _ = inner.notifier.notified() => {},
                        }
                    } else {
                        idle_backoff.reset();
                    }
                }
                Err(FetchError::NotLeader) => {
                    slog::info!(inner.logger, "leader changed, rediscovering coordinator";
                        "worker_id" => inner.config.id.clone());
                    if let Ok(new_base) = inner.coordinator_base_url().await {
                        base_url = new_base;
                        inner.clone().spawn_ws_listener(base_url.clone()).await;
                    }
                }
                Err(FetchError::Http(error)) => {
                    slog::warn!(inner.logger, "task fetch failed";
                        "worker_id" => inner.config.id.clone(),
                        "error" => %error);

                    if let Some(delay) = error_backoff.next_backoff() {
                        sleep(delay).await;
                    } else {
                        error_backoff.reset();
                    }

                    if let Ok(new_base) = inner.coordinator_base_url().await {
                        base_url = new_base;
                        inner.clone().spawn_ws_listener(base_url.clone()).await;
                    }
                }
            }
        }
    }
}

impl WorkerInner {
    async fn fetch_tasks_with_retry(&self, base_url: &Url) -> Result<Vec<Task>, FetchError> {
        let backoff = self.pull_backoff();

        retry(backoff, || async {
            match self.fetch_tasks_once(base_url).await {
                Ok(tasks) => Ok(tasks),
                Err(FetchError::NotLeader) => Err(BackoffError::Permanent(FetchError::NotLeader)),
                Err(err) => Err(BackoffError::Transient {
                    err,
                    retry_after: None,
                }),
            }
        })
        .await
    }

    async fn fetch_tasks_once(&self, base_url: &Url) -> Result<Vec<Task>, FetchError> {
        let mut url = base_url.clone();
        url.set_path("tasks/pull/bin");
        url.query_pairs_mut()
            .clear()
            .append_pair("worker_id", &self.config.id)
            .append_pair("max", &self.config.batch_size.to_string())
            .append_pair("capability", &self.config.capability.to_string());

        let response = self
            .http
            .get(url)
            .header("Accept", "application/postcard")
            .send()
            .await
            .map_err(|err| FetchError::Http(err.into()))?;

        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = response
            .bytes()
            .await
            .map_err(|err| FetchError::Http(err.into()))?;

        match status {
            StatusCode::OK => {
                if content_type.starts_with("application/postcard") {
                    let envelope = postcard::from_bytes::<TaskBatchEnvelope>(&bytes)
                        .map_err(|err| FetchError::Http(err.into()))?;
                    Ok(envelope.tasks)
                } else {
                    let envelope = serde_json::from_slice::<TaskBatchEnvelope>(&bytes)
                        .map_err(|err| FetchError::Http(err.into()))?;
                    Ok(envelope.tasks)
                }
            }
            StatusCode::NO_CONTENT => Ok(Vec::new()),
            StatusCode::CONFLICT | StatusCode::TEMPORARY_REDIRECT => Err(FetchError::NotLeader),
            status => Err(FetchError::Http(anyhow!(
                "unexpected status while fetching tasks: {status}"
            ))),
        }
    }

    async fn drain_handles(&self, handles: Vec<usize>) -> Vec<Task> {
        let mut pool = self.task_pool.lock().await;
        let mut tasks = Vec::with_capacity(handles.len());
        for handle in handles {
            if pool.get(handle).is_some() {
                tasks.push(pool.remove(handle));
            }
        }
        tasks
    }

    fn process_compute_batch(&self, tasks: Vec<Task>) -> Vec<(Task, Result<TaskOutcome>)> {
        self.thread_pool.install(|| {
            tasks
                .into_par_iter()
                .map(|task| {
                    let started = Instant::now();
                    let outcome = run_mock_inference(&task.payload)
                        .map(|(checksum, result)| TaskOutcome {
                            checksum,
                            result,
                            elapsed: started.elapsed(),
                        })
                        .map_err(anyhow::Error::from);
                    (task, outcome)
                })
                .collect()
        })
    }

    async fn process_io_task(&self, task: &Task) -> Result<TaskOutcome> {
        let started = Instant::now();

        fs::create_dir_all(SENSOR_DIR)
            .await
            .context("preparing sensor directory")?;

        let path = format!("{SENSOR_DIR}/{}-sensor.log", self.config.id);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("opening sensor log at {path}"))?;

        let averages: Vec<f32> = task
            .payload
            .chunks(8)
            .map(|chunk| chunk.iter().map(|b| *b as f32).sum::<f32>() / chunk.len().max(1) as f32)
            .collect();

        let serialized = averages
            .iter()
            .map(|v| format!("{v:.3}"))
            .collect::<Vec<_>>()
            .join(",");

        file.write_all(serialized.as_bytes()).await?;
        file.write_all(b"\n").await?;

        let checksum = payload_checksum(&task.payload);
        let result = averages.iter().copied().sum::<f32>() / averages.len().max(1) as f32;

        Ok(TaskOutcome {
            checksum,
            result,
            elapsed: started.elapsed(),
        })
    }

    fn log_task_result(&self, task: &Task, outcome: &TaskOutcome) {
        slog::info!(self.logger, "task processed";
            "worker_id" => self.config.id.clone(),
            "task_id" => task.id,
            "priority" => task.priority,
            "kind" => task.kind.to_string(),
            "checksum" => outcome.checksum,
            "result" => outcome.result,
            "elapsed_ms" => outcome.elapsed.as_millis());
    }

    async fn report_completion(
        &self,
        base_url: &Url,
        task: &Task,
        outcome: &TaskOutcome,
    ) -> Result<()> {
        let mut url = base_url.clone();
        url.set_path("tasks/complete");

        let report = CompletionReport {
            worker_id: self.config.id.clone(),
            task_id: task.id,
            checksum: outcome.checksum,
            result: outcome.result,
            elapsed_ms: outcome.elapsed.as_millis() as u64,
            status: "ok",
            kind: task.kind.clone(),
        };

        self.http
            .post(url)
            .json(&report)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    async fn spawn_ws_listener(self: Arc<Self>, base_url: Url) {
        let mut guard = self.ws_task.lock().await;
        if let Some(handle) = guard.take() {
            handle.abort();
        }

        let Some(ws_url) = Self::websocket_url(&base_url) else {
            slog::warn!(self.logger, "failed to derive websocket url";
                "coordinator" => base_url.as_str());
            return;
        };

        let logger = self.logger.clone();
        let notify = self.notifier.clone();
        let handle = tokio::spawn(async move {
            loop {
                match connect_async(ws_url.clone()).await {
                    Ok((mut socket, _)) => {
                        slog::info!(logger, "websocket connected";
                            "url" => ws_url.to_string());
                        loop {
                            match timeout(Duration::from_secs(30), socket.next()).await {
                                Ok(Some(Ok(WsMessage::Text(text)))) => {
                                    if let Ok(notification) = from_str::<QueueNotification>(&text) {
                                        match notification {
                                            QueueNotification::TaskEnqueued {
                                                task_id,
                                                priority,
                                                kind,
                                                depth,
                                            } => {
                                                notify.notify_waiters();
                                                slog::trace!(logger, "queue enqueued";
                                                    "task_id" => task_id,
                                                    "priority" => priority,
                                                    "kind" => kind.to_string(),
                                                    "depth" => depth);
                                            }
                                            QueueNotification::TaskDequeued {
                                                worker_id,
                                                count,
                                                depth,
                                            } => {
                                                notify.notify_waiters();
                                                slog::trace!(logger, "queue dequeued";
                                                    "worker_id" => worker_id.unwrap_or_else(|| "unknown".into()),
                                                    "count" => count,
                                                    "depth" => depth);
                                            }
                                            QueueNotification::QueueSnapshot { depth } => {
                                                slog::trace!(logger, "queue snapshot"; "depth" => depth);
                                            }
                                            QueueNotification::TaskCompleted {
                                                worker_id,
                                                task_id,
                                                result,
                                                elapsed_ms,
                                            } => {
                                                slog::trace!(logger, "task completed";
                                                    "worker_id" => worker_id,
                                                    "task_id" => task_id,
                                                    "result" => result,
                                                    "elapsed_ms" => elapsed_ms);
                                            }
                                        }
                                    }
                                }
                                Ok(Some(Ok(WsMessage::Close(_)))) | Ok(None) => break,
                                Ok(Some(Ok(_))) => {}
                                Ok(Some(Err(err))) => {
                                    slog::warn!(logger, "websocket receive error";
                                        "error" => %err);
                                    break;
                                }
                                Err(_) => {
                                    // heartbeat timeout, loop to reconnect
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        slog::warn!(logger, "websocket connection failed";
                            "url" => ws_url.to_string(),
                            "error" => %error);
                    }
                }

                sleep(Duration::from_secs(2)).await;
            }
        });

        *guard = Some(handle);
    }

    fn pull_backoff(&self) -> ExponentialBackoff {
        ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(100))
            .with_multiplier(1.7)
            .with_max_interval(Duration::from_secs(2))
            .with_max_elapsed_time(Some(Duration::from_secs(5)))
            .build()
    }

    fn idle_backoff() -> ExponentialBackoff {
        ExponentialBackoffBuilder::new()
            .with_initial_interval(IDLE_BACKOFF_INITIAL)
            .with_multiplier(1.5)
            .with_max_interval(MAX_BACKOFF_DELAY)
            .with_max_elapsed_time(None)
            .build()
    }

    fn error_backoff() -> ExponentialBackoff {
        ExponentialBackoffBuilder::new()
            .with_initial_interval(ERROR_BACKOFF_INITIAL)
            .with_multiplier(2.0)
            .with_max_interval(MAX_BACKOFF_DELAY)
            .with_max_elapsed_time(None)
            .build()
    }

    fn websocket_url(base: &Url) -> Option<Url> {
        let mut ws_url = base.clone();
        let scheme = match ws_url.scheme() {
            "http" => "ws".to_string(),
            "https" => "wss".to_string(),
            "ws" | "wss" => ws_url.scheme().to_string(),
            _ => return None,
        };
        ws_url.set_scheme(&scheme).ok()?;
        ws_url.set_path("ws");
        ws_url.set_query(None);
        Some(ws_url)
    }

    fn normalize_base_url(input: &str) -> Result<Url> {
        let mut url = Url::parse(input).context("invalid coordinator hint")?;
        if url.path().is_empty() {
            url.set_path("/");
        }
        if !url.path().ends_with('/') {
            let mut path = url.path().to_string();
            if !path.ends_with('/') {
                path.push('/');
            }
            url.set_path(&path);
        }
        Ok(url)
    }
}

#[derive(Deserialize)]
struct TaskBatchEnvelope {
    tasks: Vec<Task>,
}

#[derive(Debug, Error)]
enum FetchError {
    #[error("coordinator not leader")]
    NotLeader,
    #[error(transparent)]
    Http(#[from] anyhow::Error),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum QueueNotification {
    TaskEnqueued {
        task_id: u64,
        priority: u8,
        kind: TaskKind,
        depth: usize,
    },
    TaskDequeued {
        worker_id: Option<String>,
        count: usize,
        depth: usize,
    },
    TaskCompleted {
        worker_id: String,
        task_id: u64,
        result: f32,
        elapsed_ms: u64,
    },
    QueueSnapshot {
        depth: usize,
    },
}

struct TaskOutcome {
    checksum: u64,
    result: f32,
    elapsed: Duration,
}

#[derive(Serialize)]
struct CompletionReport {
    worker_id: String,
    task_id: u64,
    checksum: u64,
    result: f32,
    elapsed_ms: u64,
    status: &'static str,
    kind: TaskKind,
}

fn payload_checksum(payload: &[u8]) -> u64 {
    payload
        .iter()
        .fold(0u64, |acc, byte| acc.wrapping_add(u64::from(*byte)))
}

fn run_mock_inference(payload: &[u8]) -> Result<(u64, f32)> {
    if payload.is_empty() {
        return Ok((0, 0.0));
    }

    let checksum = payload_checksum(payload);
    let normalized: Vec<f32> = payload.iter().map(|b| *b as f32 / 255.0).collect();
    let len = normalized.len();

    let tensor =
        Tensor::from_shape(&[1, len], &normalized).context("failed to build inference tensor")?;

    let mut model = TypedModel::default();
    let input = model.add_source(
        "input",
        TypedFact::dt_shape(f32::datum_type(), tvec![1, len]),
    )?;
    model.set_output_outlets(&[input])?;

    let runnable = model.into_runnable()?;
    let result = runnable.run(tvec!(tensor.into()))?;

    let output_tensor = result
        .get(0)
        .context("missing inference output")?
        .clone()
        .into_tensor();

    let output = output_tensor.to_array_view::<f32>()?.to_owned();

    let prediction = output.iter().copied().sum::<f32>() / output.len().max(1) as f32;
    Ok((checksum, prediction))
}

fn discover_via_mdns(service_name: &str, logger: &Logger) -> Option<Url> {
    let daemon = ServiceDaemon::new().ok()?;
    let receiver = daemon.browse(service_name).ok()?;
    let deadline = Instant::now() + DISCOVERY_TIMEOUT;

    while let Ok(event) = receiver.recv_timeout(Duration::from_millis(200)) {
        match event {
            ServiceEvent::ServiceResolved(info) => {
                for addr in info.get_addresses().iter().copied() {
                    let base = format!("http://{addr}:{}", info.get_port());
                    if let Ok(url) = Url::parse(&format!("{base}/")) {
                        return Some(url);
                    }
                }
            }
            ServiceEvent::SearchStopped(_) => break,
            _ => {}
        }

        if Instant::now() >= deadline {
            break;
        }
    }

    slog::debug!(logger, "mDNS discovery timed out"; "service" => service_name);
    None
}

impl WorkerInner {
    async fn discover_coordinator(&self) -> Result<Option<Url>> {
        let service = self.config.service_name.clone();
        let logger = self.logger.clone();

        tokio::task::spawn_blocking(move || discover_via_mdns(&service, &logger))
            .await
            .context("mdns discovery task failed")
    }

    async fn coordinator_base_url(&self) -> Result<Url> {
        if let Some(hint) = &self.config.coordinator_hint {
            return Self::normalize_base_url(hint);
        }

        if let Some(url) = self.discover_coordinator().await? {
            return Ok(url);
        }

        Err(anyhow!("unable to discover coordinator"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn checksum_matches_sum(payload in proptest::collection::vec(any::<u8>(), 0..64)) {
            prop_assert_eq!(payload_checksum(&payload), payload.iter().map(|b| u64::from(*b)).sum::<u64>());
        }
    }
}

#[cfg(all(test, feature = "loom-tests"))]
mod loom_worker_tests {
    use super::*;
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    #[test]
    fn slab_allocation_concurrency() {
        loom::model(|| {
            let pool = Arc::new(Mutex::new(Slab::new()));
            let pool_insert = pool.clone();
            let t1 = thread::spawn(move || {
                let mut pool = pool_insert.lock().unwrap();
                pool.insert(Task::new(42, TaskKind::Compute, 1, vec![1, 2, 3]));
            });
            let pool_remove = pool.clone();
            let t2 = thread::spawn(move || {
                let mut pool = pool_remove.lock().unwrap();
                if let Some((handle, _)) = pool.iter().next() {
                    pool.remove(handle);
                }
            });
            t1.join().unwrap();
            t2.join().unwrap();
        });
    }
}
