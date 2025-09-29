use crate::coordinator::{COORDINATOR_PORT, SERVICE_NAME};
use crate::queue::Task;

use anyhow::{anyhow, Context, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use rayon::prelude::*;
use reqwest::{Client, StatusCode};
use slog::Logger;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(2);
const IDLE_BACKOFF: Duration = Duration::from_millis(250);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct WorkerConfig {
    pub id: String,
    pub service_name: String,
    pub coordinator_hint: Option<String>,
}

impl WorkerConfig {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            service_name: SERVICE_NAME.to_string(),
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
}

impl Worker {
    pub fn new(config: WorkerConfig, logger: Logger) -> Result<Self> {
        let http = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("failed to build worker HTTP client")?;

        Ok(Self {
            config,
            logger,
            http,
        })
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
        let mut coordinator_base_url = self
            .coordinator_base_url()
            .await
            .unwrap_or_else(|| format!("http://127.0.0.1:{COORDINATOR_PORT}/"));

        slog::info!(self.logger, "worker online"; "worker_id" => self.config.id.clone(), "coordinator" => coordinator_base_url.clone());

        loop {
            match self.fetch_next_task(&coordinator_base_url).await {
                Ok(Some(task)) => {
                    let outcome = self.process_task(&task)?;
                    if let Err(error) = self
                        .report_completion(&coordinator_base_url, &task, &outcome)
                        .await
                    {
                        slog::warn!(self.logger, "failed to report completion";
                            "worker_id" => self.config.id.clone(),
                            "task_id" => task.id,
                            "error" => %error);
                    }
                }
                Ok(None) => {
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

    fn process_task(&self, task: &Task) -> Result<TaskOutcome> {
        let started = Instant::now();
        let checksum = Self::checksum(task);
        let inference_score = Self::simulate_inference(task).unwrap_or_default();

        slog::info!(self.logger, "task processed";
            "worker_id" => self.config.id.clone(),
            "task_id" => task.id,
            "priority" => task.priority,
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
        task.payload.par_iter().map(|value| u64::from(*value)).sum()
    }

    fn simulate_inference(task: &Task) -> Result<f32> {
        use tract_core::prelude::Tensor;

        if task.payload.is_empty() {
            return Ok(0.0);
        }

        let normalized: Vec<f32> = task
            .payload
            .iter()
            .map(|value| f32::from(*value) / 255.0)
            .collect();

        let rows = normalized.len();
        let tensor = Tensor::from_shape(&[rows, 1], normalized.as_slice())
            .context("failed to construct inference tensor")?;

        let slice = tensor
            .as_slice::<f32>()
            .context("extracting tensor slice failed")?;

        Ok(slice.iter().copied().sum())
    }

    async fn discover_coordinator(&self) -> Option<SocketAddr> {
        let service = self.config.service_name.clone();
        let logger = self.logger.clone();

        tokio::task::spawn_blocking(move || discover_via_mdns(&service, &logger))
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

    async fn fetch_next_task(&self, base_url: &str) -> Result<Option<Task>> {
        let url = format!("{base_url}tasks/next?worker_id={}", self.config.id);
        let response = self.http.get(url).send().await?;

        match response.status() {
            StatusCode::OK => Ok(Some(response.json::<Task>().await?)),
            StatusCode::NO_CONTENT => Ok(None),
            status => Err(anyhow!("unexpected status while fetching task: {status}")),
        }
    }

    async fn report_completion(
        &self,
        base_url: &str,
        task: &Task,
        outcome: &TaskOutcome,
    ) -> Result<()> {
        let report = CompletionReport {
            worker_id: self.config.id.clone(),
            task_id: task.id,
            checksum: outcome.checksum,
            inference: outcome.inference_score,
            elapsed_ms: outcome.elapsed.as_millis() as u64,
        };

        let url = format!("{base_url}tasks/complete");
        self.http
            .post(url)
            .json(&report)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
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

#[derive(serde::Serialize)]
struct CompletionReport {
    worker_id: String,
    task_id: u64,
    checksum: u64,
    inference: f32,
    elapsed_ms: u64,
}
