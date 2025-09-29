use crate::consensus::LeaderState;
use crate::metrics::Metrics;
use crate::queue::{Task, TaskQueue};

use anyhow::Result;
use axum::{
    extract::{Extension, Query},
    http::{header::CONTENT_TYPE, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension as ExtensionLayer, Json, Router,
};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub const COORDINATOR_PORT: u16 = 8080;
pub const SERVICE_NAME: &str = "_edgeforge._tcp.local.";

#[derive(Clone)]
pub struct CoordinatorContext {
    queue: TaskQueue,
    metrics: Metrics,
    leader: LeaderState,
    logger: Logger,
}

impl CoordinatorContext {
    pub fn new(queue: TaskQueue, metrics: Metrics, leader: LeaderState, logger: Logger) -> Self {
        Self {
            queue,
            metrics,
            leader,
            logger,
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
        .route("/tasks", post(enqueue_task))
        .route("/tasks/next", get(next_task))
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
    let id = request.id.unwrap_or_else(|| rand::random());
    let task = Task::new(id, request.priority, request.payload);
    context.queue().push(task).await;
    StatusCode::CREATED
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
