use axum::{
    body::{self, Body},
    http::{Request, StatusCode},
};
use edgeforge::consensus;
use edgeforge::coordinator::{build_router, CoordinatorContext};
use edgeforge::metrics::Metrics;
use edgeforge::queue::TaskQueue;
use serde::Deserialize;
use serde_json::json;
use slog::o;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tower::util::ServiceExt;

#[derive(Deserialize)]
struct TaskBatchResponse {
    tasks: Vec<edgeforge::queue::Task>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_round_trip_with_partition() {
    let logger = slog::Logger::root(slog::Discard, o!());
    let cluster = consensus::bootstrap(&[1, 2], logger.clone()).expect("consensus bootstrap");
    let (node_id, node) = cluster
        .nodes
        .into_iter()
        .find(|(_, node)| node.leader.is_leader())
        .expect("leader available");

    let queue = TaskQueue::with_replicator_and_actor(
        node.replicator.clone(),
        format!("test-node-{node_id}"),
    );
    let metrics = Metrics::new().expect("metrics");
    let (tx, _rx) = broadcast::channel(32);
    let pending = Arc::new(Mutex::new(HashMap::new()));
    let context = Arc::new(CoordinatorContext::new(
        queue.clone(),
        metrics.clone(),
        node.leader.clone(),
        logger.clone(),
        format!("test-node-{node_id}"),
        tx,
        pending,
    ));

    let router = build_router(context.clone());

    // Enqueue three compute tasks via HTTP.
    for task_id in 0..3u64 {
        let body = json!({
            "id": task_id,
            "task_type": "compute",
            "priority": 5,
            "payload": [1, 2, 3, task_id as u8],
        });
        let response = router
            .clone()
            .oneshot(
                Request::post("/tasks")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    // Pull as postcard and ensure tasks are returned.
    let response = router
        .clone()
        .oneshot(
            Request::get("/tasks/pull/bin?worker_id=test-worker&max=3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let TaskBatchResponse { tasks } = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(tasks.len(), 3);

    // Complete the tasks using the coordinator API.
    for task in tasks {
        let checksum = task.checksum();
        let completion = json!({
            "worker_id": "test-worker",
            "task_id": task.id,
            "checksum": checksum,
            "elapsed_ms": 10,
            "result": 0.0,
            "kind": task.kind,
        });
        let response = router
            .clone()
            .oneshot(
                Request::post("/tasks/complete")
                    .header("content-type", "application/json")
                    .body(Body::from(completion.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    // Simulate a partition by applying a snapshot to a replica queue and reconciling.
    let snapshot_bytes = queue.export_snapshot().await;
    let replica = TaskQueue::with_actor("replica");
    replica.import_snapshot(snapshot_bytes).await;
    assert_eq!(replica.len().await, queue.len().await);

    // Metrics should record completed tasks for the worker.
    let metrics_body = router
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(metrics_body.status(), StatusCode::OK);
    let metric_text = String::from_utf8(
        body::to_bytes(metrics_body.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(metric_text.contains("edgeforge_tasks_completed"));
}
