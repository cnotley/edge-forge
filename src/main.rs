use edgeforge::consensus;
use edgeforge::coordinator::{self, CoordinatorContext, COORDINATOR_PORT};
use edgeforge::metrics::Metrics;
use edgeforge::queue::{Task, TaskQueue};
use edgeforge::worker::{Worker, WorkerConfig};

use anyhow::Result;
use rand::Rng;
use slog::{o, Drain, Logger};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let logger = build_logger();
    let queue = TaskQueue::new();

    seed_queue(&queue, 8).await?;

    let metrics = Metrics::new()?;

    let leader_state = consensus::spawn(
        vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string(),
        ],
        logger.new(o!("component" => "consensus")),
    );

    let coordinator_context = CoordinatorContext::new(
        queue.clone(),
        metrics.clone(),
        leader_state,
        logger.new(o!("component" => "coordinator")),
    );
    let coordinator_handle = coordinator::spawn(coordinator_context);

    let mut worker_handles = Vec::new();
    for worker_idx in 0..3 {
        let worker_config = WorkerConfig::new(format!("worker-{worker_idx}"))
            .with_coordinator_hint(format!("http://127.0.0.1:{COORDINATOR_PORT}/"));
        let worker = Worker::new(
            worker_config,
            logger.new(o!("component" => "worker", "id" => worker_idx)),
        )?;
        worker_handles.push(worker.spawn());
    }

    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            slog::info!(logger, "EdgeForge demonstration complete");
        }
    }

    coordinator_handle.abort();
    for worker in worker_handles {
        worker.abort();
    }

    Ok(())
}

fn build_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().stderr().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    Logger::root(drain, o!("app" => "edgeforge"))
}

async fn seed_queue(queue: &TaskQueue, count: usize) -> Result<()> {
    let mut rng = rand::thread_rng();

    for id in 0..count as u64 {
        let priority = rng.gen_range(1..=10);
        let payload: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
        let task = Task::new(id, priority, payload);
        queue.push(task).await;
    }

    Ok(())
}
