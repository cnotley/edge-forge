use crate::coordinator::{COORDINATOR_PORT, SERVICE_NAME};
use crate::queue::{Task, TaskKind, infer_task_kind};

use anyhow::{anyhow, Context, Result};
use backoff::future::retry;
use backoff::{Error as BackoffError, ExponentialBackoff};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use rand::seq::SliceRandom;
use rayon::prelude::*;
use reqwest::{Client, StatusCode};
use slog::Logger;
use crate::metrics::Metrics;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_stream::{wrappers::IntervalStream, StreamExt};
use tokio_tungstenite::connect_async;
use slab::Slab;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);
const IDLE_BACKOFF: Duration = Duration::from_millis(250);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15); // allow long-poll waits
const ALT_SERVICE_NAME: &str = "_task-scheduler._tcp.local."; // preferred; falls back to coordinator SERVICE_NAME

#[derive(Clone)]
pub struct WorkerConfig {
    pub id: String,
    pub service_names: Vec<String>,
    pub coordinator_hint: Option<String>,
}

impl WorkerConfig {
    pub fn new(id: impl Into<String>) -> Self {
        // Support both the requested _task-scheduler._tcp and the coordinator's advertised service
        let services = vec![ALT_SERVICE_NAME.to_string(), SERVICE_NAME.to_string()];
        Self {
            id: id.into(),
            service_names: services,
            coordinator_hint: None,
        }
    }

    pub fn with_coordinator_hint(mut self, hint: impl Into<String>) -> Self {
        self.coordinator_hint = Some(hint.into());
        self
    }
}

#[derive(Clone)]
pub struct Worker {
    config: WorkerConfig,
    logger: Logger,
    http: Client,
    metrics: Option<Metrics>,
    buffers: Slab<Vec<f32>>,
}

impl Worker {
    pub fn new(config: WorkerConfig, logger: Logger, metrics: Option<Metrics>) -> Result<Self> {
        // reqwest Client is cheap to clone and is Send+Sync; keep it on Arc internally
        let http = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .pool_idle_timeout(Duration::from_secs(30))
            .build()
            .context("failed to build worker HTTP client")?;

        Ok(Self { config, logger, http, metrics, buffers: Slab::with_capacity(64) })
    }

    pub fn spawn(self) -> JoinHandle<()> {
        let logger = self.logger.clone();
        let worker_id = self.config.id.clone();

        tokio::spawn(async move {
            if let Err(error) = self.run().await {
                slog::error!(logger, "worker stopped"; "worker_id" => worker_id, "error" => %error);
            }
        })
    }

    async fn run(self) -> Result<()> {
        // Deterministic base URL selection with hint > mDNS > localhost fallback
        let mut coordinator_base_url = self
            .coordinator_base_url()
            .await
            .unwrap_or_else(|| format!("http://127.0.0.1:{COORDINATOR_PORT}/"));

        slog::info!(self.logger, "worker online"; "worker_id" => self.config.id.clone(), "coordinator" => coordinator_base_url.clone());

        // Spawn websocket listener for real-time updates (best-effort)
        let ws_base = coordinator_base_url.clone().replace("http", "ws");
        let ws_logger = self.logger.clone();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = connect_async(format!("{}ws", ws_base)).await {
                while let Some(msg) = tokio_stream::StreamExt::next(&mut stream).await {
                    match msg {
                        Ok(m) => {
                            if let Ok(text) = m.to_text() {
                                slog::debug!(ws_logger, "ws"; "msg" => text);
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        });

        loop {
            // Long-poll with exponential backoff for transient errors
            let pull_started = Instant::now();
            match self.long_poll_next_task(&coordinator_base_url).await {
                Ok(Some(task)) => {
                    if let Some(m) = &self.metrics { m.observe_pull_latency(&self.config.id, pull_started.elapsed().as_secs_f64()); }
                    let outcome = self.process_task(&task).await?;
                    if let Err(error) = self.report_completion_with_backoff(&coordinator_base_url, &task, &outcome).await {
                        slog::warn!(self.logger, "failed to report completion";
                            "worker_id" => self.config.id.clone(),
                            "task_id" => task.id,
                            "error" => %error);
                    }
                    if let Some(m) = &self.metrics { m.observe_process_latency(&self.config.id, outcome.elapsed.as_secs_f64()); }
                }
                Ok(None) => {
                    // No task available; short idle backoff avoids hot-looping on empty queue
                    tokio::time::sleep(IDLE_BACKOFF).await;
                }
                Err(error) => {
                    slog::warn!(self.logger, "task fetch failed";
                        "worker_id" => self.config.id.clone(),
                        "error" => %error);
                    tokio::time::sleep(IDLE_BACKOFF).await;
                    if let Some(base_url) = self.coordinator_base_url().await {
                        coordinator_base_url = base_url;
                    }
                }
            }
        }
    }

    // Concurrency note: This method performs CPU-bound tasks with Rayon and I/O-bound tasks with async streams,
    // avoiding blocking the Tokio scheduler. Only cheap bookkeeping occurs on the async runtime thread.
    async fn process_task(&self, task: &Task) -> Result<TaskOutcome> {
        let started = Instant::now();
        let kind = infer_task_kind(task);

        let (checksum, inference_score) = match kind {
            TaskKind::Compute => {
                // Compute intensive: parallel checksum + simulated inference
                let checksum = Self::checksum(task);
                let inference = Self::simulate_inference(task).unwrap_or_default();
                (checksum, inference)
            }
            TaskKind::IoBound => {
                // Simulate streaming/aggregation work using a Tokio interval stream
                let checksum = self.aggregate_streaming_checksum(task).await?;
                (checksum, 0.0)
            }
        };

        slog::info!(self.logger, "task processed";
            "worker_id" => self.config.id.clone(),
            "task_id" => task.id,
            "priority" => task.priority,
            "kind" => format!("{:?}", kind),
            "checksum" => checksum,
            "inference" => inference_score,
        );

        Ok(TaskOutcome {
            checksum,
            inference_score,
            elapsed: started.elapsed(),
        })
    }

    fn checksum(task: &Task) -> u64 {
        // Parallel reduction with Rayon for CPU-bound path
        task.payload.par_iter().map(|value| u64::from(*value)).sum()
    }

    fn simulate_inference(task: &Task) -> Result<f32> {
        if task.payload.is_empty() {
            return Ok(0.0);
        }
        // Parallel, allocation-light inference approximation using Rayon; constant-time w.r.t data values
        let sum: f32 = task
            .payload
            .par_iter()
            .map(|value| f32::from(*value) * (1.0 / 255.0))
            .sum();
        Ok(sum)
    }

    async fn aggregate_streaming_checksum(&self, task: &Task) -> Result<u64> {
        // Simulate chunked sensor data aggregation over time using a timer-driven stream
        let mut idx = 0usize;
        let mut sum: u64 = 0;
        let interval = tokio::time::interval(Duration::from_millis(10));
        let mut stream = IntervalStream::new(interval);
        while let Some(_) = stream.next().await {
            for _ in 0..8 {
                if idx >= task.payload.len() {
                    return Ok(sum);
                }
                sum += u64::from(task.payload[idx]);
                idx += 1;
            }
        }
        Ok(sum)
    }

    async fn discover_coordinator(&self) -> Option<SocketAddr> {
        let services = self.config.service_names.clone();
        let logger = self.logger.clone();

        tokio::task::spawn_blocking(move || {
            // Shuffle to avoid thundering herd on one service name
            let mut names = services;
            names.shuffle(&mut rand::thread_rng());
            for name in names {
                if let Some(addr) = discover_via_mdns(&name, &logger) {
                    return Some(addr);
                }
            }
            None
        })
        .await
        .ok()
        .flatten()
    }

    async fn coordinator_base_url(&self) -> Option<String> {
        if let Some(hint) = &self.config.coordinator_hint {
            return Some(hint.clone());
        }
        self.discover_coordinator()
            .await
            .map(|addr| format!("http://{addr}/"))
    }

    async fn long_poll_next_task(&self, base_url: &str) -> Result<Option<Task>> {
        // Prefer long-poll endpoint; fall back to /tasks/next for compatibility
        let pull_url = format!("{base_url}tasks/pull?worker_id={}&max=10", self.config.id);
        let next_url = format!("{base_url}tasks/next?worker_id={}", self.config.id);

        let op = || async {
            let response = self
                .http
                .get(&pull_url)
                .send()
                .await
                .map_err(|e| BackoffError::transient(anyhow!(e)))?;
            match response.status() {
                StatusCode::OK => {
                    // Support both batch and single response
                    let bytes = response.bytes().await.map_err(|e| BackoffError::transient(anyhow!(e)))?;
                    // Try Vec<Task> first
                    if let Ok(tasks) = serde_json::from_slice::<Vec<Task>>(&bytes) {
                        if let Some(task) = tasks.into_iter().next() {
                            return Ok(Some(task));
                        } else {
                            return Ok(None);
                        }
                    }
                    let task: Task = serde_json::from_slice(&bytes)
                        .map_err(|e| BackoffError::transient(anyhow!(e)))?;
                    Ok(Some(task))
                }
                StatusCode::NO_CONTENT => Ok(None),
                StatusCode::NOT_FOUND => {
                    // Fallback path
                    let resp = self
                        .http
                        .get(&next_url)
                        .send()
                        .await
                        .map_err(|e| BackoffError::transient(anyhow!(e)))?;
                    match resp.status() {
                        StatusCode::OK => {
                            let task = resp
                                .json::<Task>()
                                .await
                                .map_err(|e| BackoffError::transient(anyhow!(e)))?;
                            Ok(Some(task))
                        }
                        StatusCode::NO_CONTENT => Ok(None),
                        status => Err(BackoffError::permanent(anyhow!("unexpected status while fetching task: {status}"))),
                    }
                }
                // Treat 409/307 as transient leader change indicators
                status if status == StatusCode::CONFLICT || status == StatusCode::TEMPORARY_REDIRECT => {
                    Err(BackoffError::transient(anyhow!("leader change")))
                }
                status => Err(BackoffError::transient(anyhow!("transient status while fetching task: {status}"))),
            }
        };

        let mut policy = ExponentialBackoff {
            initial_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(2),
            max_elapsed_time: Some(Duration::from_secs(5)),
            ..ExponentialBackoff::default()
        };

        retry(policy, op).await
    }

    async fn report_completion_with_backoff(&self, base_url: &str, task: &Task, outcome: &TaskOutcome) -> Result<()> {
        let url = format!("{base_url}tasks/complete");
        let report = CompletionReport {
            worker_id: self.config.id.clone(),
            task_id: task.id,
            checksum: outcome.checksum,
            inference: outcome.inference_score,
            elapsed_ms: outcome.elapsed.as_millis() as u64,
        };

        let op = || async {
            let resp = self
                .http
                .post(&url)
                .json(&report)
                .send()
                .await
                .map_err(|e| BackoffError::transient(anyhow!(e)))?;
            match resp.error_for_status_ref() {
                Ok(_) => Ok(()),
                Err(err) => {
                    // Treat 4xx as permanent to avoid retry storms
                    if let Some(status) = err.status() {
                        if status.is_client_error() {
                            return Err(BackoffError::permanent(anyhow!("client error: {status}")));
                        }
                    }
                    Err(BackoffError::transient(anyhow!("server/transport error")))
                }
            }
        };

        let policy = ExponentialBackoff::default();
        retry(policy, op).await
    }
}

fn discover_via_mdns(service_name: &str, logger: &Logger) -> Option<SocketAddr> {
    let daemon = ServiceDaemon::new().ok()?;
    let receiver = daemon.browse(service_name).ok()?;
    let deadline = Instant::now() + DISCOVERY_TIMEOUT;

    while let Ok(event) = receiver.recv_timeout(Duration::from_millis(200)) {
        match event {
            ServiceEvent::ServiceResolved(info) => {
                for addr in info.get_addresses().iter().copied() {
                    return Some(SocketAddr::new(addr, info.get_port()));
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

struct TaskOutcome {
    checksum: u64,
    inference_score: f32,
    elapsed: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn checksum_is_sum(payload in proptest::collection::vec(any::<u8>(), 0..256)) {
            let task = Task::new(1, 1, payload.clone());
            let sum: u64 = payload.into_iter().map(|v| v as u64).sum();
            prop_assert_eq!(Worker::checksum(&task), sum);
        }
    }
}

#[derive(serde::Serialize)]
struct CompletionReport {
    worker_id: String,
    task_id: u64,
    checksum: u64,
    inference: f32,
    elapsed_ms: u64,
}
