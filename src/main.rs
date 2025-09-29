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

    // Simple runtime mode selection: default demo, or worker-only when invoked with `--worker`
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--worker") {
        let worker_id = args
            .windows(2)
            .find(|w| w[0] == "--id")
            .map(|w| w[1].clone())
            .unwrap_or_else(|| format!("worker-{}", rand::thread_rng().gen::<u16>()));
        let hint = args
            .windows(2)
            .find(|w| w[0] == "--hint")
            .map(|w| w[1].clone());

        let mut config = WorkerConfig::new(worker_id);
        if let Some(h) = hint { config = config.with_coordinator_hint(h); }
        let worker = Worker::new(config, logger.new(o!("component" => "worker")), None)?;
        let handle = worker.spawn();
        handle.await.ok();
        return Ok(());
    }

    // Scale simulation mode: --nodes N --seed M --duration S
    if let Some(idx) = args.iter().position(|a| a == "--nodes") {
        if let Some(nodes_s) = args.get(idx + 1) {
            let nodes: usize = nodes_s.parse().unwrap_or(100);
            let seed = args
                .windows(2)
                .find(|w| w[0] == "--seed")
                .and_then(|w| w[1].parse::<usize>().ok())
                .unwrap_or(nodes * 50);
            let duration_secs = args
                .windows(2)
                .find(|w| w[0] == "--duration")
                .and_then(|w| w[1].parse::<u64>().ok())
                .unwrap_or(10);

            seed_queue(&queue, seed).await?;
            let metrics = Metrics::new()?;
            let (leader_state, replicator) = consensus::spawn_cluster(
                vec![
                    "node-a".to_string(),
                    "node-b".to_string(),
                    "node-c".to_string(),
                ],
                logger.new(o!("component" => "consensus")),
                Some(metrics.clone()),
            );
            let coordinator_context = CoordinatorContext::new(
                queue.clone(),
                metrics.clone(),
                leader_state,
                replicator,
                logger.new(o!("component" => "coordinator")),
            );
            let coordinator_handle = coordinator::spawn(coordinator_context);

            let mut worker_handles = Vec::new();
            for worker_idx in 0..nodes {
                let worker_config = WorkerConfig::new(format!("worker-{worker_idx}"))
                    .with_coordinator_hint(format!("http://127.0.0.1:{COORDINATOR_PORT}/"));
                let worker = Worker::new(
                    worker_config,
                    logger.new(o!("component" => "worker", "id" => worker_idx)),
                    Some(metrics.clone()),
                )?;
                worker_handles.push(worker.spawn());
            }

            tokio::time::sleep(Duration::from_secs(duration_secs)).await;
            let rendered = metrics.render()?;
            let mut completed_total: f64 = 0.0;
            for line in rendered.lines() {
                if line.starts_with("edgeforge_tasks_completed") && !line.starts_with('#') {
                    if let Some(val) = line.split_whitespace().last() {
                        if let Ok(n) = val.parse::<f64>() {
                            completed_total += n;
                        }
                    }
                }
            }
            let tps = completed_total / (duration_secs as f64);
            slog::info!(logger, "scale run complete"; "nodes" => nodes, "seed" => seed, "duration_s" => duration_secs, "completed" => completed_total, "tps" => tps);

            coordinator_handle.abort();
            for worker in worker_handles { worker.abort(); }
            return Ok(());
        }
    }

    seed_queue(&queue, 8).await?;

    let metrics = Metrics::new()?;

    let (leader_state, replicator) = consensus::spawn_cluster(
        vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string(),
        ],
        logger.new(o!("component" => "consensus")),
        Some(metrics.clone()),
    );

    let coordinator_context = CoordinatorContext::new(
        queue.clone(),
        metrics.clone(),
        leader_state,
        replicator,
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
            Some(metrics.clone()),
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
