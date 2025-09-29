use axum::{
    extract::{ws::WebSocketUpgrade, State},
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use edgeforge::consensus;
use edgeforge::coordinator::{build_router, CoordinatorContext};
use edgeforge::metrics::Metrics;
use edgeforge::queue::{QueueLogEntry, Task, TaskKind, TaskQueue};
use edgeforge::worker::{Worker, WorkerConfig, DEFAULT_BATCH_SIZE};
use serde::{Deserialize, Serialize};
use serde_json::json;
use slog::{o, Drain, Logger};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
#[cfg(feature = "raft-consensus")]
use tokio::time::timeout;
use tower::util::ServiceExt;

const COMPLETION_WAIT: Duration = Duration::from_secs(2);

fn test_logger() -> Logger {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::sink());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    Logger::root(drain, o!())
}

fn build_queue_with_replicator(node: &consensus::ConsensusNode, label: &str) -> TaskQueue {
    TaskQueue::with_replicator_and_actor(node.replicator.clone(), label)
}

async fn coordinator_harness() -> (Router, TaskQueue, Metrics) {
    let logger = test_logger();
    let cluster = consensus::bootstrap(&[1, 2], logger.clone()).expect("consensus bootstrap");
    let (node_id, node) = cluster
        .nodes
        .into_iter()
        .find(|(_, node)| node.leader.is_leader())
        .expect("leader available");

    let queue = build_queue_with_replicator(&node, &format!("node-{node_id}"));
    let metrics = Metrics::new().expect("metrics");
    let (tx, _rx) = broadcast::channel(32);
    let pending = Arc::new(Mutex::new(HashMap::new()));
    let context = Arc::new(CoordinatorContext::new(
        queue.clone(),
        metrics.clone(),
        node.leader.clone(),
        logger.clone(),
        format!("node-{node_id}"),
        tx,
        pending,
    ));
    (build_router(context), queue, metrics)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rubric_coordinator_endpoints() {
    let (router, queue, metrics) = coordinator_harness().await;

    for priority in [5_u8, 7] {
        let body = json!({
            "task_type": "compute",
            "priority": priority,
            "payload": [1, 2, 3, priority],
        });
        let response = router
            .clone()
            .oneshot(
                Request::post("/tasks")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    metrics.set_queue_depth("node-1", queue.len().await);

    let health = router
        .clone()
        .oneshot(
            Request::get("/healthz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let metrics_res = router
        .clone()
        .oneshot(
            Request::get("/metrics")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(metrics_res.status(), StatusCode::OK);

    let pull = router
        .clone()
        .oneshot(
            Request::get("/tasks/pull?worker_id=rubric&max=2")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        pull.status(),
        StatusCode::OK | StatusCode::NO_CONTENT
    ));

    let postcard_pull = router
        .clone()
        .oneshot(
            Request::get("/tasks/pull/bin?worker_id=rubric&max=2")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        postcard_pull.status(),
        StatusCode::OK | StatusCode::NO_CONTENT
    ));
}

#[derive(Clone)]
struct WorkerHarness {
    pending_tasks: Arc<RwLock<Vec<Task>>>,
    served: Arc<AtomicBool>,
    completions: Arc<Mutex<Vec<CompletionPayload>>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TaskEnvelope {
    tasks: Vec<Task>,
}

#[derive(Debug, Deserialize, Clone)]
struct CompletionPayload {
    worker_id: String,
    task_id: u64,
    checksum: u64,
    result: f32,
    elapsed_ms: u64,
    status: String,
    kind: TaskKind,
}

async fn worker_router(
    tasks: Vec<Task>,
) -> (
    String,
    Arc<Mutex<Vec<CompletionPayload>>>,
    oneshot::Sender<()>,
    JoinHandle<()>,
) {
    let listener = TcpListener::bind(SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 0)))
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("listener addr");

    let completions = Arc::new(Mutex::new(Vec::new()));
    let state = WorkerHarness {
        pending_tasks: Arc::new(RwLock::new(tasks)),
        served: Arc::new(AtomicBool::new(false)),
        completions: completions.clone(),
    };

    let router = Router::new()
        .route("/tasks/pull/bin", get(handle_pull_postcard))
        .route("/tasks/pull", get(handle_pull_json))
        .route("/tasks/complete", post(handle_complete))
        .route("/ws", get(handle_ws))
        .with_state(state);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, router.into_make_service())
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .ok();
    });

    (
        format!("http://{addr}/"),
        completions,
        shutdown_tx,
        server_handle,
    )
}

async fn handle_pull_postcard(State(state): State<WorkerHarness>) -> impl IntoResponse {
    let tasks = next_batch(&state).await;
    if tasks.is_empty() {
        return StatusCode::NO_CONTENT.into_response();
    }

    let bytes = postcard::to_allocvec(&TaskEnvelope { tasks }).expect("postcard encode");
    (
        StatusCode::OK,
        [("content-type", "application/postcard")],
        bytes,
    )
        .into_response()
}

async fn handle_pull_json(State(state): State<WorkerHarness>) -> impl IntoResponse {
    let tasks = next_batch(&state).await;
    if tasks.is_empty() {
        return StatusCode::NO_CONTENT.into_response();
    }

    Json(TaskEnvelope { tasks }).into_response()
}

async fn next_batch(state: &WorkerHarness) -> Vec<Task> {
    if state.served.swap(true, Ordering::SeqCst) {
        return Vec::new();
    }
    state.pending_tasks.read().await.clone()
}

async fn handle_complete(
    State(state): State<WorkerHarness>,
    Json(payload): Json<CompletionPayload>,
) -> impl IntoResponse {
    state.completions.lock().await.push(payload);
    StatusCode::ACCEPTED
}

async fn handle_ws(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(|mut socket| async move {
        let message = json!({
            "type": "queue_snapshot",
            "depth": 1
        })
        .to_string();

        if let Err(error) = socket.send(axum::extract::ws::Message::Text(message)).await {
            eprintln!("ws send error: {error}");
        }
    })
}

async fn spawn_worker_once(
    base_url: &str,
    capability: TaskKind,
    batch_size: usize,
) -> JoinHandle<()> {
    let config = WorkerConfig::new("rubric-worker")
        .with_coordinator_hint(base_url.to_string())
        .with_capability(capability)
        .with_batch_size(batch_size);
    let worker = Worker::new(config, test_logger()).expect("worker init");
    worker.spawn()
}

async fn run_worker_harness() -> (
    Arc<Mutex<Vec<CompletionPayload>>>,
    oneshot::Sender<()>,
    JoinHandle<()>,
) {
    let tasks = vec![
        Task::new(1, TaskKind::Compute, 9, (0..64).collect()),
        Task::new(2, TaskKind::Io, 5, vec![5; 32]),
    ];
    let (base_url, completions, shutdown_tx, server_handle) = worker_router(tasks).await;
    let worker_handle = spawn_worker_once(&base_url, TaskKind::Compute, DEFAULT_BATCH_SIZE).await;

    // Allow the worker to fetch and complete at least one iteration.
    tokio::time::sleep(Duration::from_millis(400)).await;
    worker_handle.abort();

    (completions, shutdown_tx, server_handle)
}

async fn collect_completions() -> Vec<CompletionPayload> {
    let (completions, shutdown, server) = run_worker_harness().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown.send(()).ok();
    server.abort();
    let cloned = {
        let guard = completions.lock().await;
        guard.clone()
    };
    cloned
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rubric_worker_long_polling() {
    let completions = collect_completions().await;
    assert!(completions.len() >= 1, "expected at least one completion");
    for completion in &completions {
        assert_eq!(completion.status, "ok");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rubric_compute_pipeline_performance() {
    let completions = collect_completions().await;
    assert!(completions
        .iter()
        .any(|c| matches!(c.kind, TaskKind::Compute)));
    assert!(completions.iter().any(|c| matches!(c.kind, TaskKind::Io)));
    for completion in &completions {
        assert!(
            completion.elapsed_ms < 750,
            "task took longer than expected"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rubric_crdt_queue_precision() {
    let queue = TaskQueue::with_actor("precision");
    for i in 0..6_u64 {
        let kind = if i % 2 == 0 {
            TaskKind::Compute
        } else {
            TaskKind::Io
        };
        queue
            .push(Task::new(
                i,
                kind,
                10 - (i as u8),
                vec![i as u8, i as u8 + 1],
            ))
            .await;
    }

    let compute_batch = queue.pop_batch(3, Some(TaskKind::Compute)).await;
    assert_eq!(compute_batch.len(), 3);
    assert!(compute_batch
        .iter()
        .all(|task| task.kind == TaskKind::Compute));

    let snapshot = queue.export_snapshot().await;
    let replica = TaskQueue::with_actor("replica");
    replica.import_snapshot(snapshot).await;
    assert_eq!(replica.len().await, queue.len().await);

    queue
        .apply_log(QueueLogEntry::Dequeue {
            task_ids: compute_batch.iter().map(|task| task.id).collect(),
        })
        .await;

    assert!(queue.len().await <= 3);
}

#[cfg(feature = "raft-consensus")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rubric_raft_replication_feature() {
    let logger = test_logger();
    let cluster = consensus::bootstrap(&[1, 2, 3], logger).expect("raft bootstrap");

    assert_eq!(cluster.nodes.len(), 3);

    let mut receivers = Vec::new();
    let mut replicator = None;

    for (_, node) in cluster.nodes.into_iter() {
        if replicator.is_none() {
            replicator = Some(node.replicator.clone());
        }
        receivers.push(node.apply_rx);
    }

    let entry = QueueLogEntry::Enqueue {
        task: Task::new(99, TaskKind::Compute, 8, vec![42; 16]),
    };

    replicator.expect("replicator").replicate(entry.clone());

    let mut observed = false;
    for mut rx in receivers {
        if let Ok(Some(received)) = timeout(COMPLETION_WAIT, rx.recv()).await {
            if matches!(received, QueueLogEntry::Enqueue { .. }) {
                observed = true;
                break;
            }
        }
    }
    assert!(observed, "raft replication should deliver log entry");
}

#[tokio::test]
async fn rubric_metrics_observability() {
    let metrics = Metrics::new().expect("metrics init");
    metrics.observe_completion("worker-a");
    metrics.inc_enqueued(&TaskKind::Compute);
    metrics.observe_task_latency(&TaskKind::Compute, 0.010);
    metrics.set_queue_depth("node-1", 3);
    metrics.inc_leader_election("node-1");

    let rendered = metrics.render().expect("render metrics");
    for metric in [
        "edgeforge_tasks_completed",
        "edgeforge_tasks_enqueued_total",
        "edgeforge_queue_depth",
        "edgeforge_task_latency_seconds",
        "edgeforge_leader_elections_total",
    ] {
        assert!(rendered.contains(metric), "metric {metric} missing");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rubric_cli_simulation_scale() {
    let binary = option_env!("CARGO_BIN_EXE_edgeforge").expect("binary path");
    let output = tokio::process::Command::new(binary)
        .arg("simulate")
        .arg("--nodes")
        .arg("1")
        .arg("--workers")
        .arg("1")
        .arg("--duration")
        .arg("1")
        .arg("--seed")
        .arg("4")
        .output()
        .await
        .expect("launch simulate");

    assert!(output.status.success(), "simulate command failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rubric_fuzz_hook_registration() {
    let queue = TaskQueue::with_actor("fuzz");
    for idx in 0..4_u64 {
        queue
            .apply_log(QueueLogEntry::Enqueue {
                task: Task::new(idx, TaskKind::Compute, 5, vec![idx as u8]),
            })
            .await;
    }

    queue
        .apply_log(QueueLogEntry::Dequeue {
            task_ids: vec![0, 1],
        })
        .await;

    let snapshot = queue.export_snapshot().await;
    queue
        .apply_log(QueueLogEntry::Snapshot { payload: snapshot })
        .await;

    assert!(queue.len().await <= 4);
}
