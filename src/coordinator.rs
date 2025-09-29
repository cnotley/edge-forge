use crate::consensus::{LeaderInfo, LeaderState};
use crate::metrics::Metrics;
use crate::queue::{Task, TaskKind, TaskQueue};

use anyhow::Result;
use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Extension, Query,
    },
    http::{header::CONTENT_TYPE, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension as ExtensionLayer, Json, Router,
};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use serde::{Deserialize, Serialize};
use serde_json::json;
use slog::Logger;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;

pub const COORDINATOR_PORT: u16 = 8080;
pub const SERVICE_NAME: &str = "_task-scheduler._tcp.local.";
const MAX_PAYLOAD_SIZE: usize = 64 * 1024;

#[derive(Clone)]
pub struct CoordinatorContext {
    queue: TaskQueue,
    metrics: Metrics,
    leader: LeaderState,
    logger: Logger,
    node_label: String,
    broadcaster: broadcast::Sender<QueueNotification>,
    pending_checksums: Arc<Mutex<HashMap<u64, u64>>>,
}

impl CoordinatorContext {
    pub fn new(
        queue: TaskQueue,
        metrics: Metrics,
        leader: LeaderState,
        logger: Logger,
        node_label: impl Into<String>,
        broadcaster: broadcast::Sender<QueueNotification>,
        pending_checksums: Arc<Mutex<HashMap<u64, u64>>>,
    ) -> Self {
        Self {
            queue,
            metrics,
            leader,
            logger,
            node_label: node_label.into(),
            broadcaster,
            pending_checksums,
        }
    }

    pub fn queue(&self) -> &TaskQueue {
        &self.queue
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn leader(&self) -> &LeaderState {
        &self.leader
    }

    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    pub fn node_label(&self) -> &str {
        &self.node_label
    }

    pub fn subscribe(&self) -> broadcast::Receiver<QueueNotification> {
        self.broadcaster.subscribe()
    }

    pub fn notify(&self, notification: QueueNotification) {
        let _ = self.broadcaster.send(notification);
    }

    pub async fn record_checksum(&self, task_id: u64, checksum: u64) {
        let mut guard = self.pending_checksums.lock().await;
        guard.insert(task_id, checksum);
    }

    pub async fn take_checksum(&self, task_id: u64) -> Option<u64> {
        let mut guard = self.pending_checksums.lock().await;
        guard.remove(&task_id)
    }

    pub fn leader_info(&self) -> LeaderInfo {
        self.leader.snapshot()
    }

    pub fn is_leader(&self) -> bool {
        self.leader.is_leader()
    }

    pub fn ensure_leader(&self) -> Result<(), CoordinatorError> {
        if self.is_leader() {
            Ok(())
        } else {
            Err(CoordinatorError::not_leader(self.leader_info()))
        }
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QueueNotification {
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

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("validation error: {reason}")]
    Validation { reason: String },
    #[error("internal error: {reason}")]
    Internal { reason: String },
    #[error(
        "not leader: leader_id={leader_id} term={term}",
        leader_id = .leader.leader_id,
        term = .leader.term
    )]
    NotLeader { leader: LeaderInfo },
}

impl CoordinatorError {
    fn validation(reason: impl Into<String>) -> Self {
        Self::Validation {
            reason: reason.into(),
        }
    }

    fn internal(reason: impl Into<String>) -> Self {
        Self::Internal {
            reason: reason.into(),
        }
    }

    fn not_leader(info: LeaderInfo) -> Self {
        Self::NotLeader { leader: info }
    }
}

impl IntoResponse for CoordinatorError {
    fn into_response(self) -> Response {
        let status = match &self {
            CoordinatorError::Validation { .. } => StatusCode::BAD_REQUEST,
            CoordinatorError::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            CoordinatorError::NotLeader { .. } => StatusCode::CONFLICT,
        };

        let message = self.to_string();
        let mut response = (status, Json(ErrorBody { error: message })).into_response();

        if let CoordinatorError::NotLeader { leader } = self {
            if let Ok(value) = HeaderValue::from_str(&leader.leader_id.to_string()) {
                response
                    .headers_mut()
                    .insert("x-edgeforge-leader-id", value);
            }
            if let Ok(value) = HeaderValue::from_str(&leader.term.to_string()) {
                response
                    .headers_mut()
                    .insert("x-edgeforge-leader-term", value);
            }
        }

        response
    }
}

pub fn spawn(context: CoordinatorContext, addr: SocketAddr) -> JoinHandle<()> {
    let logger = context.logger().clone();
    tokio::spawn(async move {
        if let Err(error) = serve(context, addr).await {
            slog::error!(logger, "coordinator server exited"; "error" => %error);
        }
    })
}

pub fn build_router(context: Arc<CoordinatorContext>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_endpoint))
        .route("/tasks", post(enqueue_task))
        .route("/tasks/pull", get(pull_task))
        .route("/tasks/pull/bin", get(pull_task_postcard))
        .route("/tasks/next", get(pull_task))
        .route("/tasks/complete", post(complete_task))
        .route("/ws", get(websocket_handler))
        .layer(ExtensionLayer(context))
}

async fn serve(context: CoordinatorContext, addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let shared_context = Arc::new(context);
    let _mdns_guard = advertise_service(shared_context.logger(), addr);
    let router = build_router(shared_context);

    let make_service = router.into_make_service();
    axum::serve(listener, make_service).await?;

    Ok(())
}

fn advertise_service(logger: &Logger, addr: SocketAddr) -> Option<ServiceDaemon> {
    let daemon = ServiceDaemon::new().ok()?;

    let host_label = format!("edgeforge-{}.local.", addr.ip());
    let ip_string = addr.ip().to_string();
    let properties = [("path", "/healthz"), ("tasks", "/tasks/pull")];

    let service_info = ServiceInfo::new(
        SERVICE_NAME,
        "edgeforge-coordinator",
        &host_label,
        &ip_string,
        addr.port(),
        &properties[..],
    )
    .ok()?;

    if let Err(error) = daemon.register(service_info) {
        slog::warn!(logger, "failed to register mDNS service"; "error" => %error);
        return None;
    }

    slog::info!(logger, "coordinator published via mDNS"; "service" => SERVICE_NAME, "addr" => %addr);
    Some(daemon)
}

#[derive(Serialize)]
struct HealthResponse {
    leader_id: u64,
    term: u64,
    is_leader: bool,
    queued_tasks: usize,
}

async fn healthz(Extension(context): Extension<Arc<CoordinatorContext>>) -> impl IntoResponse {
    let queue_depth = context.queue().len().await;
    context
        .metrics()
        .set_queue_depth(context.node_label(), queue_depth);
    context.notify(QueueNotification::QueueSnapshot { depth: queue_depth });
    let leader = context.leader_info();
    let response = HealthResponse {
        leader_id: leader.leader_id,
        term: leader.term,
        is_leader: context.is_leader(),
        queued_tasks: queue_depth,
    };
    Json(response)
}

#[derive(Debug, Deserialize)]
struct TaskSubmission {
    #[serde(default)]
    id: Option<u64>,
    #[serde(rename = "task_type")]
    kind: TaskKind,
    priority: u8,
    #[serde(default)]
    payload: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct TaskAccepted {
    task_id: u64,
    queue_depth: usize,
}

#[derive(Debug, Serialize)]
struct TaskBatchResponse {
    tasks: Vec<Task>,
}

async fn enqueue_task(
    Extension(context): Extension<Arc<CoordinatorContext>>,
    Json(request): Json<TaskSubmission>,
) -> Result<impl IntoResponse, CoordinatorError> {
    context.ensure_leader()?;

    let TaskSubmission {
        id,
        kind,
        priority,
        payload,
    } = request;

    validate_task_submission(priority, &payload)?;

    let task_id = id.unwrap_or_else(rand::random);
    let task = Task::new(task_id, kind.clone(), priority, payload);

    context.queue().push(task).await;
    context.metrics().inc_enqueued(&kind);
    let queue_depth = context.queue().len().await;
    context
        .metrics()
        .set_queue_depth(context.node_label(), queue_depth);
    context.notify(QueueNotification::TaskEnqueued {
        task_id,
        priority,
        kind: kind.clone(),
        depth: queue_depth,
    });

    slog::info!(context.logger(), "task enqueued";
        "task_id" => task_id,
        "kind" => kind.to_string(),
        "priority" => priority,
        "queue_depth" => queue_depth,
    );

    let response = TaskAccepted {
        task_id,
        queue_depth,
    };

    Ok((StatusCode::CREATED, Json(response)))
}

#[derive(Default, Deserialize)]
struct PullTaskQuery {
    worker_id: Option<String>,
    max: Option<usize>,
    capability: Option<TaskKind>,
}

async fn pull_task(
    Extension(context): Extension<Arc<CoordinatorContext>>,
    Query(params): Query<PullTaskQuery>,
) -> Result<Response, CoordinatorError> {
    context.ensure_leader()?;

    if let Some(worker) = params.worker_id.as_ref() {
        if worker.trim().is_empty() {
            return Err(CoordinatorError::validation("worker_id must not be empty"));
        }
        sanitize_worker_id(worker)?;
    }

    let PullTaskQuery {
        worker_id,
        max,
        capability,
    } = params;

    let batch_size = max.unwrap_or(5).clamp(1, 10);
    let tasks = context
        .queue()
        .pop_batch(batch_size, capability.clone())
        .await;

    let queue_depth = context.queue().len().await;
    context
        .metrics()
        .set_queue_depth(context.node_label(), queue_depth);

    if tasks.is_empty() {
        context.notify(QueueNotification::QueueSnapshot { depth: queue_depth });
        return Ok(StatusCode::NO_CONTENT.into_response());
    }

    if let Some(worker) = worker_id.as_ref() {
        slog::debug!(context.logger(), "task batch issued";
            "worker_id" => worker.as_str(),
            "tasks" => tasks.len(),
            "capability" => capability.map(|k| k.to_string()).unwrap_or_else(|| "any".into()),
        );
    }

    let worker_label = worker_id.clone();

    for task in &tasks {
        context.record_checksum(task.id, task.checksum()).await;
    }

    context.notify(QueueNotification::TaskDequeued {
        worker_id: worker_label,
        count: tasks.len(),
        depth: queue_depth,
    });

    Ok((StatusCode::OK, Json(TaskBatchResponse { tasks })).into_response())
}

async fn pull_task_postcard(
    Extension(context): Extension<Arc<CoordinatorContext>>,
    Query(params): Query<PullTaskQuery>,
) -> Result<Response, CoordinatorError> {
    context.ensure_leader()?;

    if let Some(worker) = params.worker_id.as_ref() {
        if worker.trim().is_empty() {
            return Err(CoordinatorError::validation("worker_id must not be empty"));
        }
        sanitize_worker_id(worker)?;
    }

    let PullTaskQuery {
        worker_id,
        max,
        capability,
    } = params;

    let batch_size = max.unwrap_or(5).clamp(1, 10);
    let tasks = context
        .queue()
        .pop_batch(batch_size, capability.clone())
        .await;

    let queue_depth = context.queue().len().await;
    context
        .metrics()
        .set_queue_depth(context.node_label(), queue_depth);

    if tasks.is_empty() {
        context.notify(QueueNotification::QueueSnapshot { depth: queue_depth });
        return Ok(StatusCode::NO_CONTENT.into_response());
    }

    if let Some(worker) = worker_id.as_ref() {
        slog::debug!(context.logger(), "task batch issued";
            "worker_id" => worker.as_str(),
            "tasks" => tasks.len(),
            "capability" => capability.map(|k| k.to_string()).unwrap_or_else(|| "any".into()),
        );
    }

    let worker_label = worker_id.clone();

    for task in &tasks {
        context.record_checksum(task.id, task.checksum()).await;
    }

    context.notify(QueueNotification::TaskDequeued {
        worker_id: worker_label,
        count: tasks.len(),
        depth: queue_depth,
    });

    let payload = postcard::to_allocvec(&TaskBatchResponse { tasks }).map_err(|error| {
        CoordinatorError::internal(format!("failed to encode postcard batch: {error}"))
    })?;

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/postcard")
        .body(Body::from(payload))
        .map_err(|error| CoordinatorError::internal(format!("failed to build response: {error}")))
}

async fn metrics_endpoint(
    Extension(context): Extension<Arc<CoordinatorContext>>,
) -> Result<Response, CoordinatorError> {
    match context.metrics().render() {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/plain; version=0.0.4")
            .body(body.into())
            .map_err(|error| {
                CoordinatorError::internal(format!("failed to build metrics response: {error}"))
            }),
        Err(error) => {
            slog::warn!(context.logger(), "failed to render metrics"; "error" => %error);
            Err(CoordinatorError::internal("metrics renderer failed"))
        }
    }
}

#[derive(Deserialize)]
struct CompletionReport {
    worker_id: String,
    task_id: u64,
    checksum: u64,
    elapsed_ms: u64,
    result: f32,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    kind: Option<TaskKind>,
}

async fn complete_task(
    Extension(context): Extension<Arc<CoordinatorContext>>,
    Json(report): Json<CompletionReport>,
) -> Result<StatusCode, CoordinatorError> {
    context.ensure_leader()?;

    let CompletionReport {
        worker_id,
        task_id,
        checksum,
        elapsed_ms,
        result,
        status,
        kind,
    } = report;

    if worker_id.trim().is_empty() {
        return Err(CoordinatorError::validation("worker_id must not be empty"));
    }
    sanitize_worker_id(&worker_id)?;

    let task_kind = kind.unwrap_or(TaskKind::Compute);
    context.metrics().observe_completion(&worker_id);
    context
        .metrics()
        .observe_task_latency(&task_kind, elapsed_ms as f64 / 1000.0);

    if let Some(expected) = context.take_checksum(task_id).await {
        if expected.ct_eq(&checksum).unwrap_u8() == 0 {
            slog::warn!(context.logger(), "checksum mismatch";
                "worker_id" => worker_id.as_str(),
                "task_id" => task_id,
                "expected" => expected,
                "reported" => checksum);
            return Err(CoordinatorError::validation("checksum mismatch"));
        }
    } else {
        slog::warn!(context.logger(), "missing checksum entry";
            "worker_id" => worker_id.as_str(),
            "task_id" => task_id);
    }

    context.notify(QueueNotification::TaskCompleted {
        worker_id: worker_id.clone(),
        task_id,
        result,
        elapsed_ms,
    });
    slog::info!(context.logger(), "task completion received";
        "worker_id" => worker_id.as_str(),
        "task_id" => task_id,
        "checksum" => checksum,
        "result" => result,
        "elapsed_ms" => elapsed_ms,
        "status" => status.unwrap_or_else(|| "unknown".into()),
        "kind" => task_kind.to_string(),
    );

    Ok(StatusCode::ACCEPTED)
}

fn validate_task_submission(priority: u8, payload: &[u8]) -> Result<(), CoordinatorError> {
    if priority == 0 {
        return Err(CoordinatorError::validation(
            "priority must be greater than zero",
        ));
    }
    if payload.len() > MAX_PAYLOAD_SIZE {
        return Err(CoordinatorError::validation("payload too large"));
    }
    Ok(())
}

fn sanitize_worker_id(worker_id: &str) -> Result<(), CoordinatorError> {
    if worker_id.len() > 128 {
        return Err(CoordinatorError::validation(
            "worker_id exceeds maximum length",
        ));
    }
    if !worker_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(CoordinatorError::validation(
            "worker_id contains invalid characters",
        ));
    }
    Ok(())
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    Extension(context): Extension<Arc<CoordinatorContext>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| websocket_loop(socket, context))
}

async fn websocket_loop(mut socket: WebSocket, context: Arc<CoordinatorContext>) {
    let mut rx = context.subscribe();

    let initial_depth = context.queue().len().await;
    let initial = QueueNotification::QueueSnapshot {
        depth: initial_depth,
    };
    if socket
        .send(Message::Text(
            serde_json::to_string(&initial).unwrap_or_else(|_| json!({}).to_string()),
        ))
        .await
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {
                        // Ignore client-originated messages for now.
                    }
                    Some(Err(_)) => break,
                }
            }
            notification = rx.recv() => {
                match notification {
                    Ok(event) => {
                        let payload = serde_json::to_string(&event)
                            .unwrap_or_else(|_| json!({}).to_string());
                        if socket.send(Message::Text(payload)).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
}
