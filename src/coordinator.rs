use crate::consensus::{LeaderState, Replicator};
use crate::metrics::Metrics;
use crate::queue::{infer_task_kind, QueueOp, Task, TaskKind, TaskQueue};

use anyhow::Result;
use axum::{
    extract::{Extension, Query},
    http::{header::CONTENT_TYPE, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension as ExtensionLayer, Json, Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::sync::broadcast;

pub const COORDINATOR_PORT: u16 = 8080;
pub const SERVICE_NAME: &str = "_edgeforge._tcp.local.";

#[derive(Clone)]
pub struct CoordinatorContext {
    queue: TaskQueue,
    metrics: Metrics,
    leader: LeaderState,
    replicator: Replicator,
    logger: Logger,
    ws_tx: broadcast::Sender<String>,
}

impl CoordinatorContext {
    pub fn new(
        queue: TaskQueue,
        metrics: Metrics,
        leader: LeaderState,
        replicator: Replicator,
        logger: Logger,
    ) -> Self {
        let (ws_tx, _rx) = broadcast::channel(128);
        Self {
            queue,
            metrics,
            leader,
            replicator,
            logger,
            ws_tx,
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

    pub fn replicator(&self) -> &Replicator {
        &self.replicator
    }

    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    pub fn ws_tx(&self) -> &broadcast::Sender<String> {
        &self.ws_tx
    }
}

pub fn spawn(context: CoordinatorContext) -> JoinHandle<()> {
    let logger = context.logger().clone();
    tokio::spawn(async move {
        if let Err(error) = serve(context).await {
            slog::error!(logger, "coordinator server exited"; "error" => %error);
        }
    })
}

async fn serve(context: CoordinatorContext) -> Result<()> {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, COORDINATOR_PORT));
    let listener = TcpListener::bind(addr).await?;
    let shared_context = Arc::new(context);
    let _mdns_guard = advertise_service(shared_context.logger(), addr);

    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_endpoint))
        .route("/ws", get(ws_handler))
        .route("/tasks", post(enqueue_task))
        .route("/tasks/next", get(next_task))
        .route("/tasks/pull", get(pull_tasks))
        .route("/tasks/complete", post(complete_task))
        .layer(ExtensionLayer(shared_context));

    let make_service = router.into_make_service();
    axum::serve(listener, make_service).await?;

    Ok(())
}

fn advertise_service(logger: &Logger, addr: SocketAddr) -> Option<ServiceDaemon> {
    let daemon = ServiceDaemon::new().ok()?;

    let host_label = format!("edgeforge-{}.local.", addr.ip());
    let ip_string = addr.ip().to_string();
    let properties = [("path", "/healthz")];

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
    leader: String,
    queued_tasks: usize,
}

async fn healthz(Extension(context): Extension<Arc<CoordinatorContext>>) -> impl IntoResponse {
    let response = HealthResponse {
        leader: context.leader().snapshot(),
        queued_tasks: context.queue().len().await,
    };
    Json(response)
}

#[derive(Deserialize)]
struct PushTaskRequest {
    id: Option<u64>,
    priority: u8,
    payload: Vec<u8>,
}

async fn enqueue_task(
    Extension(context): Extension<Arc<CoordinatorContext>>,
    Json(request): Json<PushTaskRequest>,
) -> impl IntoResponse {
    // Leader-only submissions
    if !context.leader().is_leader("node-a") {
        // In a real system we would include leader address for redirect
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error":"not leader","leader": context.leader().snapshot()})),
        )
            .into_response();
    }
    // OWASP-style input validation: constrain priority and payload size
    if request.priority > 100 || request.payload.len() > 64 * 1024 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"invalid input","reason":"priority>100 or payload too large"})),
        )
            .into_response();
    }
    let id = request.id.unwrap_or_else(|| rand::random());
    let task = Task::new(id, request.priority, request.payload);
    // Replicate enqueue
    let op = QueueOp::Enqueue(task.clone());
    if let Ok(bytes) = op.to_postcard() {
        let _ = context.replicator().replicate(bytes).await;
    }
    // Metrics: enqueue and queue depth (compute kind before moving task)
    let kind = infer_task_kind(&task);
    context.queue().push(task).await;
    // CRDT state export for zero-copy merge downstream (stub replicator)
    if let Ok(state) = context.queue().crdt_export().await {
        let _ = context.replicator().replicate(state).await;
    }
    context.metrics().observe_enqueued(match kind { TaskKind::Compute => "compute", TaskKind::IoBound => "io" }, 1);
    context.metrics().set_queue_depth(context.queue().len().await as i64);
    let _ = context.ws_tx().send(format!("enqueued:{{\"id\":{},\"kind\":\"{}\"}}", id, match kind { TaskKind::Compute => "compute", TaskKind::IoBound => "io" }));
    StatusCode::CREATED.into_response()
}

#[derive(Default, Deserialize)]
struct NextTaskQuery {
    worker_id: Option<String>,
}

async fn next_task(
    Extension(context): Extension<Arc<CoordinatorContext>>,
    Query(params): Query<NextTaskQuery>,
) -> impl IntoResponse {
    match context.queue().pop().await {
        Some(task) => {
            if let Some(worker) = params.worker_id {
                slog::debug!(context.logger(), "task issued"; "worker_id" => worker, "task_id" => task.id);
            }
            (StatusCode::OK, Json(task)).into_response()
        }
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

#[derive(Default, Deserialize)]
struct PullTasksQuery {
    max: Option<usize>,
    kind: Option<String>,
    worker_id: Option<String>,
}

async fn pull_tasks(
    Extension(context): Extension<Arc<CoordinatorContext>>,
    Query(params): Query<PullTasksQuery>,
) -> impl IntoResponse {
    let max = params.max.unwrap_or(5).clamp(1, 10);
    let kind = params.kind.as_deref().and_then(|k| match k {
        "compute" => Some(TaskKind::Compute),
        "io" => Some(TaskKind::IoBound),
        _ => None,
    });

    let tasks = context.queue().pop_batch(max, kind).await;
    if tasks.is_empty() {
        return StatusCode::NO_CONTENT.into_response();
    }

    // Replicate dequeue by ids
    let ids: Vec<u64> = tasks.iter().map(|t| t.id).collect();
    let op = QueueOp::DequeueBatch { task_ids: ids.clone() };
    if let Ok(bytes) = op.to_postcard() {
        let _ = context.replicator().replicate(bytes).await;
    }

    if let Some(worker) = params.worker_id {
        for task in &tasks {
            slog::debug!(context.logger(), "task issued"; "worker_id" => worker.clone(), "task_id" => task.id);
        }
    }

    // Metrics: dequeued and queue depth
    let kind_label = match kind { Some(TaskKind::Compute) => "compute", Some(TaskKind::IoBound) => "io", None => "any" };
    context.metrics().observe_dequeued(kind_label, tasks.len() as u64);
    context.metrics().set_queue_depth(context.queue().len().await as i64);
    let _ = context.ws_tx().send(format!("dequeued_batch:{{\"count\":{},\"kind\":\"{}\"}}", tasks.len(), kind_label));

    (StatusCode::OK, Json(tasks)).into_response()
}

async fn ws_handler(
    Extension(context): Extension<Arc<CoordinatorContext>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| websocket_loop(context, socket))
}

async fn websocket_loop(context: Arc<CoordinatorContext>, mut socket: WebSocket) {
    let mut rx = context.ws_tx().subscribe();
    while let Ok(msg) = rx.recv().await {
        if socket
            .send(Message::Text(msg.clone()))
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn metrics_endpoint(
    Extension(context): Extension<Arc<CoordinatorContext>>,
) -> impl IntoResponse {
    match context.metrics().render() {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/plain; version=0.0.4")
            .body(body.into())
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()),
        Err(error) => {
            slog::warn!(context.logger(), "failed to render metrics"; "error" => %error);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(Deserialize)]
struct CompletionReport {
    worker_id: String,
    task_id: u64,
    checksum: u64,
    inference: f32,
    elapsed_ms: u64,
}

async fn complete_task(
    Extension(context): Extension<Arc<CoordinatorContext>>,
    Json(report): Json<CompletionReport>,
) -> impl IntoResponse {
    context.metrics().observe_completion(&report.worker_id);
    slog::info!(context.logger(), "task completion received";
        "worker_id" => report.worker_id,
        "task_id" => report.task_id,
        "checksum" => report.checksum,
        "inference" => report.inference,
        "elapsed_ms" => report.elapsed_ms,
    );

    StatusCode::ACCEPTED
}
